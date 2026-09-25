import hashlib, json, logging, time
from collections import OrderedDict
import requests
from platforms.base import PlatformBase
from core.models import SellOffer, Match, Contract, OfferStatus, Platform
log = logging.getLogger("bot.peach")

# Peach escrowVersion 2: single-sig, key-path-only P2TR escrow owned by the
# seller alone. Anything lower is the legacy 2-of-2 P2WSH escrow shared with
# Peach, which still exists for older contracts.
ESCROW_VERSION = 2

class LRUCache(OrderedDict):
    """Simple LRU cache based on OrderedDict."""
    def __init__(self, maxsize=100):
        super().__init__()
        self.maxsize = maxsize
    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            self.popitem(last=False)


class PeachPlatform(PlatformBase):
    name = "peach"
    escrow_type = "onchain"

    def __init__(self, config):
        self.base_url = config.get("api_base", "https://api.peachbitcoin.com/v1")
        self.base_url_v069 = self.base_url.replace("/v1", "/v069")
        self.private_key_hex = config.get("private_key_hex", "")
        self.mnemonic = config.get("mnemonic", "")
        self.network = config.get("network", "mainnet")
        self.unique_id = config.get("unique_id", "")
        self.refund_address = config.get("refund_address", "")
        self.pgp_public_key = config.get("pgp_public_key", "")
        self.pgp_private_key = config.get("pgp_private_key", "")
        self.payment_data_raw = config.get("payment_data_raw", {})  # {method: raw_value}
        self.session = requests.Session()
        # The real Peach client sends its build number on every request (header name is
        # literally "buildNumber", see Peach2Peach/peach-api-ts getPublicHeaders), so we
        # match it. NOTE: this is NOT what fixes the 401 {"error":"Please update the App to
        # the latest version"} on POST /v1/offer — that message is Peach's response to a
        # missing `escrowVersion: 2` field (mandatory since the single-sig taproot escrow
        # release, 2026-09-01) and persists with a current build number. Don't chase the
        # header again. Configurable so a future app release is a config bump.
        self.build_number = str(config.get("build_number", 396))
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "buildNumber": self.build_number,
        })
        self.access_token = None
        self.user_id = ""
        self._auth_time = 0
        # Store escrow keypairs per offer for PSBT signing
        self.escrow_keys = {}
        self._contract_method_cache = LRUCache(100)  # {contract_id: payment_method}
        self._scan_cache = {}  # {"sell"|"buy": {"data": [...], "ts": float}}
        self._scan_cache_ttl = 60  # seconds
        self._escrow_status_cache = {}  # {offer_id: {"data": dict, "ts": float}}
        self._escrow_status_cache_ttl = 300  # 5 minutes
        self._auth_lock = __import__('threading').Lock()  # prevents concurrent re-auths

    def _get_escrow_privkey_hex(self, offer_id) -> str:
        """
        Return the escrow private key for offer_id.
        If mnemonic is configured: derives m/84'/0'/0'/{offer_id}' (matching Peach app).
        Falls back to account key (old behaviour) if no mnemonic.
        """
        if self.mnemonic:
            from core.hd_keys import get_peach_escrow_privkey
            return get_peach_escrow_privkey(self.mnemonic, offer_id, self.network)
        return self.escrow_keys.get(str(offer_id), self.private_key_hex)

    # ─── AUTH ──────────────────────────────────────────────

    def authenticate(self):
        from coincurve import PrivateKey
        privkey = PrivateKey(bytes.fromhex(self.private_key_hex))
        pubkey_hex = privkey.public_key.format(compressed=True).hex()
        self.user_id = pubkey_hex  # Peach userId == our compressed pubkey; needed for /user/{id}/offers
        log.info(f"Peach auth as {pubkey_hex[:16]}...")

        ts = str(int(time.time() * 1000))
        message = f"Peach Registration {ts}"
        msg_hash = hashlib.sha256(message.encode()).digest()
        signature = privkey.sign_recoverable(msg_hash, hasher=None)[:64].hex()

        auth_payload = {
            "publicKey": pubkey_hex,
            "uniqueId": self.unique_id,
            "message": message,
            "signature": signature
        }
        r = self.session.post(f"{self.base_url}/user/auth", json=auth_payload, timeout=15)
        r.raise_for_status()
        data = r.json()
        self.access_token = data.get("accessToken")
        self.session.headers["Authorization"] = f"Bearer {self.access_token}"
        self._auth_time = time.time()
        log.info("Peach auth OK")
        return True

    def _ensure_auth(self):
        """Re-authenticate if token might be expired (tokens last ~1h).
        Thread-safe: double-checked locking prevents concurrent re-auths."""
        if not self.access_token or (time.time() - self._auth_time > 3000):
            with self._auth_lock:
                if not self.access_token or (time.time() - self._auth_time > 3000):
                    self.authenticate()

    def _api_call(self, method, url, quiet_statuses=(), **kwargs):
        """Make API call with automatic 401 retry, 5xx backoff, and transient
        network-error (Timeout/ConnectionError) retry so a short network blip doesn't
        fail a whole poll cycle. _api_call is used for idempotent GETs (+ cancel), so
        retrying these is safe.

        `quiet_statuses`: error statuses the caller expects and logs itself — logged here
        at DEBUG instead of WARNING. Still raised as usual."""
        self._ensure_auth()
        kwargs.setdefault("timeout", 15)
        def _do():
            last = None
            for attempt in range(3):
                try:
                    return self.session.request(method, url, **kwargs)
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    last = e
                    if attempt < 2:
                        log.info(f"Peach: {method} {url} network error ({type(e).__name__}), retry {attempt+1}/2")
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    raise
            raise last
        r = _do()
        if r.status_code == 401:
            # A 401 is normally an expired token -> re-auth and retry. But a 401 that arrives
            # right after a *fresh* login is not a token problem: it's a terminal/forbidden
            # resource (e.g. a sold/expired sell offer whose tradeRequestReceived 401s). Re-authing
            # per such 401 caused an auth storm once the match-poll window widened to cover many dead
            # offers. Only re-auth when the token is actually old enough to plausibly be expired;
            # otherwise fast-fail so the caller can skip the dead resource.
            if time.time() - self._auth_time > 30:
                log.info("Peach: 401, re-authenticating...")
                self.authenticate()
                r = _do()
        if r.status_code >= 500:
            # Retry once after short delay on server errors
            time.sleep(2)
            r = _do()
        if r.status_code >= 400:
            _lvl = log.debug if r.status_code in quiet_statuses else log.warning
            _lvl(f"Peach: {method} {url} -> {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
        return r

    # ─── OFFERS (v1) ───────────────────────────────────────

    def create_sell_offer(self, min_sats, max_sats, premium_pct, payment_methods, **kw):
        self._ensure_auth()
        # Peach API v1 requires amount as a single integer (max_sats), not [min, max]
        payload = {
            "type": "ask",
            "amount": max_sats,
            # Mandatory since Peach's single-sig escrow release (2026-09-01). A
            # request without it is answered with a misleading
            # 401 "Please update the App to the latest version".
            "escrowVersion": ESCROW_VERSION,
            "premium": premium_pct,
            "meansOfPayment": payment_methods,
            "returnAddress": self.refund_address
        }
        if self.pgp_public_key:
            payload["pgpPublicKey"] = self.pgp_public_key

        # Build paymentData hashes from config's payment_data_raw (required by Peach API)
        payment_data = kw.get("payment_data", {})
        if not payment_data and self.payment_data_raw:
            for method, raw in self.payment_data_raw.items():
                # Peach hashes each FIELD's value on its own (lowercased), NOT a JSON blob. For
                # revolut/wise the hashed field is `userName` -> hash the raw userName string directly.
                payment_data[method] = {"hashes": [make_payment_hash(raw)]}
        if payment_data:
            # Collect all methods actually used in meansOfPayment
            all_offered = set()
            for methods in payload["meansOfPayment"].values():
                all_offered.update(methods)
            # Filter paymentData to only include methods in meansOfPayment
            payment_data = {m: v for m, v in payment_data.items() if m in all_offered}
            # Filter meansOfPayment to only include methods that have a payment hash.
            # Offering sepa/revolut without their hash causes Peach to return 400.
            filtered_mop = {}
            for currency, methods in payload["meansOfPayment"].items():
                valid = [m for m in methods if m in payment_data]
                if valid:
                    filtered_mop[currency] = valid
            if filtered_mop:
                payload["meansOfPayment"] = filtered_mop
            payload["paymentData"] = payment_data

        log.info(f"Peach: Sell {max_sats} sats, {premium_pct}%")

        r = self._api_call("POST", f"{self.base_url}/offer", json=payload)
        data = r.json()
        oid = data.get("offerId", data.get("id", ""))
        log.info(f"Peach: Offer created: {oid}")

        esc = data.get("escrow", {})
        return SellOffer(
            id=oid, platform=Platform.PEACH, status=OfferStatus.CREATED,
            premium_pct=premium_pct, min_sats=min_sats, max_sats=max_sats,
            escrow_address=esc.get("address", "") if esc else "",
            created_at=data.get("createdAt", ""), raw_data=data
        )

    def create_escrow(self, offer_id, public_key_hex=None):
        """Create escrow for an offer. Derives per-offer key if mnemonic is set."""
        if not public_key_hex:
            from coincurve import PrivateKey
            escrow_hex = self._get_escrow_privkey_hex(offer_id)
            escrow_privkey = PrivateKey(bytes.fromhex(escrow_hex))
            public_key_hex = escrow_privkey.public_key.format(compressed=True).hex()
            self.escrow_keys[str(offer_id)] = escrow_hex
            log.info(f"Peach: escrow key for {offer_id} derived (mnemonic={'yes' if self.mnemonic else 'no'})")

        log.info(f"Peach: Creating escrow for {offer_id}...")
        r = self._api_call("POST", f"{self.base_url}/offer/{offer_id}/escrow", json={
            "publicKey": public_key_hex
        })
        data = r.json()

        escrow_addr = ""
        peach_pubkey = ""
        if "escrows" in data:
            escrow_addr = data["escrows"].get("bitcoin", "")
        elif "escrow" in data:
            escrow_addr = data["escrow"] if isinstance(data["escrow"], str) else data["escrow"].get("address", "")

        if "escrowPeachPublicKey" in data:
            peach_pubkey = data["escrowPeachPublicKey"].get("bitcoin", "")

        # escrowVersion 2 escrows are single-sig: whoever owns the key owns the
        # coins. Peach tells us the address, but funding an address that does not
        # derive from our own escrow key would hand the sats to someone else, so
        # verify it locally before anything can be sent there. A legacy P2WSH
        # address (witness v0) is left to the old 2-of-2 flow.
        if escrow_addr:
            from core.taproot import is_p2tr_address, verify_escrow_address
            if is_p2tr_address(escrow_addr):
                if not verify_escrow_address(escrow_addr, bytes.fromhex(public_key_hex), self.network):
                    raise ValueError(
                        f"Peach returned taproot escrow {escrow_addr} which does not derive "
                        f"from our escrow key for offer {offer_id} — refusing to use it")
                log.info(f"Peach: escrow {offer_id} verified as our single-sig taproot address")

        log.info(f"Peach: Escrow address: {escrow_addr[:40]}...")
        if peach_pubkey:
            log.debug(f"Peach: escrow pubkey: {peach_pubkey[:20]}...")

        return {
            "address": escrow_addr,
            "peach_pubkey": peach_pubkey,
            "raw": data
        }

    def get_escrow_address(self, offer_id):
        r = self._api_call("GET", f"{self.base_url}/offer/{offer_id}/escrow", timeout=10)
        data = r.json()
        return data.get("address", data.get("escrows", {}).get("bitcoin", ""))

    def get_escrow_status(self, offer_id, use_cache=True):
        """Returns the escrow info dict including funding.status (cached 5min)"""
        if use_cache:
            cached = self._escrow_status_cache.get(str(offer_id))
            if cached and (time.time() - cached["ts"]) < self._escrow_status_cache_ttl:
                return cached["data"]
        r = self._api_call("GET", f"{self.base_url}/offer/{offer_id}/escrow", timeout=10)
        data = r.json()
        self._escrow_status_cache[str(offer_id)] = {"data": data, "ts": time.time()}
        return data

    def get_active_offers(self):
        # Peach removed GET /v1/offers (404 since 2026-07-20). The working, AUTHORITATIVE replacement
        # is GET /v069/user/{userId}/offers: it returns exactly our own currently-active sell offers
        # (bot-created AND manually created in the app), each with its published paymentData hashes.
        # This is the real source of truth — no local reconstruction, so it cannot be blinded and it
        # covers manual offers.
        self._ensure_auth()
        if not self.user_id:
            self.authenticate()
        r = self._api_call("GET", f"{self.base_url_v069}/user/{self.user_id}/offers", timeout=15)
        data = r.json()
        raw_offers = data.get("sellOffers", []) if isinstance(data, dict) else (data or [])
        offers = []
        for o in raw_offers:
            if o.get("type") != "ask":
                continue
            if not o.get("online", True):  # only live/matchable offers
                continue
            amt = o.get("amount")
            if isinstance(amt, list):
                min_s = amt[0] if amt else 0
                max_s = amt[1] if len(amt) > 1 else 0
            else:
                min_s, max_s = 0, (amt or 0)
            funding_status = (o.get("funding") or {}).get("status", "")
            if o.get("matches"):
                status = OfferStatus.MATCHED
            elif funding_status == "FUNDED":
                status = OfferStatus.FUNDED
            else:
                status = OfferStatus.CREATED
            offers.append(SellOffer(
                id=o.get("id", ""),
                platform=Platform.PEACH,
                status=status,
                premium_pct=o.get("premium", 0),
                min_sats=min_s, max_sats=max_s,
                raw_data=o
            ))
        return offers

    def cancel_offer(self, offer_id):
        self._api_call("POST", f"{self.base_url}/offer/{offer_id}/cancel", timeout=10)
        return True

    def update_premium(self, offer_id, new_premium):
        """Update premium on an active (online) offer. Returns True on success."""
        try:
            self._api_call("PATCH", f"{self.base_url}/offer/{offer_id}",
                json={"premium": new_premium})
            log.info(f"Peach: updated offer {offer_id} premium to {new_premium}%")
            return True
        except Exception as e:
            log.warning(f"Peach: PATCH offer {offer_id} premium: {e}")
            return False

    def get_offer(self, offer_id):
        """Get single offer details"""
        r = self._api_call("GET", f"{self.base_url}/offer/{offer_id}", timeout=10)
        return r.json()

    # ─── TRADE REQUESTS via v069 (NEW - this solves the match problem!) ──

    def check_trade_requests(self, offer_id):
        """
        Check for incoming trade requests using v069 API.
        This is the WORKING alternative to the broken v1 matches endpoint.
        
        Returns list of trade requests with buyer info.
        """
        self._ensure_auth()
        url = f"{self.base_url_v069}/sellOffer/{offer_id}/tradeRequestReceived"
        log.debug(f"Peach: Checking trade requests for {offer_id}...")
        # 401 is how a sold/expired offer answers, and the match poll plus the auto-accept
        # watchdog re-sweep every tracked offer — the callers log it at the level it deserves.
        r = self._api_call("GET", url, quiet_statuses=(401,))
        data = r.json()

        if isinstance(data, list):
            requests_list = data
        elif isinstance(data, dict):
            requests_list = data.get("matches", [])
        else:
            requests_list = []

        if requests_list:
            log.info(f"Peach: Found {len(requests_list)} trade request(s) for {offer_id}")
            for tr in requests_list:
                uid = tr.get("userId", tr.get("user", {}).get("id", "unknown"))
                method = tr.get("paymentMethod", "?")
                currency = tr.get("currency", "?")
                amount = tr.get("amount", 0)
                has_sym = bool(tr.get("symmetricKeyEncrypted"))
                log.info(f"Peach:   -> {uid[:12]}... wants {amount} sats via {method} ({currency}), symKey: {has_sym}")

        return requests_list

    def accept_trade_request(self, offer_id, buyer_user_id, payment_method, payment_data_raw, trade_request_data=None, payment_info=None, published_hash=None):
        """
        Accept a trade request from a buyer (v069 API).

        Args:
            offer_id: Our sell offer ID
            buyer_user_id: The buyer's userId from tradeRequestReceived
            payment_method: e.g. "twint", "revolut", "sepa"
            payment_data_raw: The raw single-field value for hashing (IBAN / phone / email / userName)
            trade_request_data: Full trade request dict (for PGP encryption)
            payment_info: Structured payment data dict for encryption (e.g. {"iban": "...", "beneficiary": "..."})
            published_hash: The hash the OFFER already committed for this method — sent verbatim so the
                accept matches the offer (incl. offers created before the lowercase-hash fix). Without
                it we compute Peach's canonical sha256(value.toLowerCase()).
        """
        payment_hash = published_hash or hashlib.sha256(payment_data_raw.lower().encode()).hexdigest()
        url = f"{self.base_url_v069}/sellOffer/{offer_id}/tradeRequestReceived/{buyer_user_id}/accept"

        payload = {
            "paymentData": {
                payment_method: {
                    "hashes": [payment_hash]
                }
            }
        }

        # Encrypt payment data using Peach's symmetric key scheme:
        # 1. Buyer sends symmetricKeyEncrypted (AES key encrypted with our PGP key)
        # 2. We decrypt the AES key with our PGP private key
        # 3. We encrypt our payment data with the AES key (PGP symmetric)
        # 4. We sign the symmetric key with our PGP private key
        if self.pgp_private_key and trade_request_data:
            try:
                import pgpy
                sym_key_encrypted = trade_request_data.get("symmetricKeyEncrypted", "")
                if sym_key_encrypted:
                    bot_privkey, _ = pgpy.PGPKey.from_blob(self.pgp_private_key)

                    # Decrypt the symmetric key
                    pgp_msg = pgpy.PGPMessage.from_blob(sym_key_encrypted)
                    sym_key = bot_privkey.decrypt(pgp_msg).message
                    log.info(f"Peach: decrypted symmetric key for offer {offer_id}")

                    # Encrypt structured payment info (Peach expects field-level data, not raw string)
                    encrypt_data = payment_info if payment_info else {payment_method: payment_data_raw}
                    payment_json = json.dumps(encrypt_data)
                    log.info(f"Peach: encrypting payment info keys: {list(encrypt_data.keys())}")

                    # Sign the payment data, then encrypt with symmetric key
                    pgp_payment_msg = pgpy.PGPMessage.new(payment_json)
                    payment_sig = bot_privkey.sign(pgp_payment_msg)
                    payload["paymentDataSignature"] = str(payment_sig)

                    encrypted_msg = pgpy.PGPMessage.new(payment_json).encrypt(sym_key)
                    payload["paymentDataEncrypted"] = str(encrypted_msg)

                    log.info(f"Peach: payment data AES-encrypted for buyer {buyer_user_id[:12]}")
                else:
                    log.warning(f"Peach: no symmetricKeyEncrypted in trade request for {offer_id}")
            except Exception as e:
                log.error(f"Peach: payment encryption failed: {e}")
                raise RuntimeError(f"Payment encryption failed — buyer will not receive payment data: {e}") from e

        log.info(f"Peach: Accepting trade from {buyer_user_id[:12]}... (offer {offer_id})")
        r = self._api_call("POST", url, json=payload)
        data = r.json()
        log.info(f"Peach: Trade accepted! Response: {json.dumps(data)[:200]}")
        return data

    # ─── MATCHES via v1 (kept for backward compat, often returns empty) ──

    def check_matches(self, offer_id):
        """v1 matches endpoint - often returns 0. Use check_trade_requests() instead."""
        self._ensure_auth()
        r = self.session.get(f"{self.base_url}/offer/{offer_id}/matches", timeout=10)
        r.raise_for_status()
        data = r.json()
        ml = data if isinstance(data, list) else data.get("matches", [])
        matches = []
        for m in ml:
            mid = m if isinstance(m, str) else m.get("offerId", "")
            if mid:
                matches.append(Match(
                    id=mid, offer_id=offer_id, platform=Platform.PEACH,
                    raw_data=m if isinstance(m, dict) else {}
                ))
        return matches

    def accept_match(self, offer_id, match_id):
        """v1 match accept - may return 404. Use accept_trade_request() instead."""
        r = self.session.post(f"{self.base_url}/offer/match", json={
            "offerId": offer_id, "matchingOfferId": match_id
        }, timeout=10)
        r.raise_for_status()
        return True

    # ─── CONTRACTS ─────────────────────────────────────────

    def get_contracts(self):
        # /contracts/summary grows with trade history — it is ~400 KB and takes a
        # steady ~9s, so a 10s timeout sat right on the edge: every overrun triggered
        # the three-attempt retry with backoff and turned a 9s call into well over a
        # minute, which is what dominated the tick.
        r = self._api_call("GET", f"{self.base_url}/contracts/summary", timeout=60)
        contracts = []
        sm = {
            "paymentRequired": OfferStatus.MATCHED,
            "paymentMade": OfferStatus.PAYMENT_RECEIVED,
            "confirmPaymentRequired": OfferStatus.PAYMENT_RECEIVED,
            "released": OfferStatus.COMPLETED,
            "dispute": OfferStatus.DISPUTE,
            "tradeCompleted": OfferStatus.COMPLETED,
            "tradeCanceled": OfferStatus.CANCELLED,
            "canceledAfterPayment": OfferStatus.CANCELLED,
            "refundTxSignatureRequired": OfferStatus.CANCELLED,
            "refundOrReviveRequired": OfferStatus.CANCELLED,
        }
        for c in r.json():
            contract_id = c.get("id", c.get("contractId", ""))
            payment_method = c.get("paymentMethod", "")
            # Summary endpoint lacks paymentMethod - use cache or fetch once
            if not payment_method and contract_id:
                payment_method = self._contract_method_cache.get(contract_id, "")
                if not payment_method:
                    try:
                        detail = self._api_call("GET", f"{self.base_url}/contract/{contract_id}", timeout=10)
                        d = detail.json()
                        payment_method = d.get("paymentMethod", "")
                        if payment_method:
                            self._contract_method_cache[contract_id] = payment_method
                    except Exception:
                        pass
            status_str = c.get("tradeStatus", c.get("status", ""))
            mapped_status = sm.get(status_str)
            if mapped_status is None and status_str:
                log.warning(f"Peach: unknown contract status '{status_str}' for {contract_id}")
                mapped_status = OfferStatus.MATCHED
            contracts.append(Contract(
                id=contract_id,
                offer_id=c.get("offerId", ""),
                platform=Platform.PEACH,
                status=mapped_status or OfferStatus.MATCHED,
                amount_sats=c.get("amount", 0),
                price_fiat=c.get("price", 0),
                currency=c.get("currency", "EUR"),
                payment_method=payment_method,
                raw_data=c
            ))
        return contracts

    def get_contract_detail(self, contract_id):
        """Get full contract details including PSBT for release signing"""
        r = self._api_call("GET", f"{self.base_url}/contract/{contract_id}")
        return r.json()

    def _single_sig_release_payload(self, contract_id, contract):
        """Build the release payload for an escrowVersion 2 (single-sig taproot) contract.

        Peach builds and broadcasts the transaction itself, so it wants the bare
        64-byte schnorr signature over the PSBT it hands out — not a finalized
        transaction. The PSBT is verified against our funding transaction and the
        buyer's payout address before it is signed: with a single-sig escrow our
        signature alone moves the coins.
        """
        from release_escrow import sign_single_sig_release
        offer_id = contract.get("offerId", "")
        release_address = contract.get("releaseAddress", "")
        if not release_address:
            raise RuntimeError(f"contract {contract_id} has no releaseAddress — refusing to sign")

        esc = self._api_call("GET", f"{self.base_url}/offer/{offer_id}/escrow", timeout=20).json()
        funding_txids = (esc.get("funding") or {}).get("txIds", [])
        if not funding_txids:
            raise RuntimeError(f"offer {offer_id} has no funding txIds — cannot verify the release PSBT")

        r = self._api_call("GET", f"{self.base_url}/contract/{contract_id}/signedReleasePSBT", timeout=20)
        psbt_b64 = r.json().get("releasePsbt")
        if not psbt_b64:
            raise RuntimeError(f"signedReleasePSBT returned no PSBT for {contract_id}")

        escrow_hex = self._get_escrow_privkey_hex(offer_id) if offer_id else self.private_key_hex
        sig_hex = sign_single_sig_release(psbt_b64, escrow_hex, funding_txids, release_address)
        log.info(f"Peach: signed single-sig taproot release for {contract_id} (offer {offer_id})")
        return {"releaseTransactionSignature": sig_hex}

    def confirm_payment(self, contract_id):
        """Confirm payment received and sign release PSBT to properly close the trade in Peach"""
        self._ensure_auth()

        payload = {}
        try:
            data = self.get_contract_detail(contract_id)
            trade_status = data.get('tradeStatus', '?')
            batch_psbt = data.get('batchReleasePsbt')
            release_psbt = data.get('releasePsbt')
            log.info(f"Peach: confirm {contract_id} tradeStatus={trade_status} batchPSBT={bool(batch_psbt)} releasePSBT={bool(release_psbt)}")

            # escrowVersion 2 contracts carry no PSBT of their own: Peach serves the
            # exact transaction it will broadcast from a separate endpoint and only
            # verifies our signature against it. Falling through to the legacy branch
            # would post an empty confirm and the trade would never release.
            escrow_version = data.get('escrowVersion')
            is_single_sig = (escrow_version == ESCROW_VERSION
                             or (escrow_version is None and not batch_psbt and not release_psbt))

            if is_single_sig:
                payload = self._single_sig_release_payload(contract_id, data)
            elif batch_psbt or release_psbt:
                from coincurve import PrivateKey
                from release_escrow import sign_psbt, build_finalized_tx, get_signing_key_from_psbt
                offer_id = data.get("offerId", "")
                derived_hex = self._get_escrow_privkey_hex(offer_id) if offer_id else self.private_key_hex
                # Auto-detect correct key from PSBT witness_script (handles old=account-key contracts)
                psbt_to_check = batch_psbt or release_psbt
                escrow_hex = get_signing_key_from_psbt(psbt_to_check, derived_hex, self.private_key_hex)
                key_src = "derived" if escrow_hex == derived_hex and escrow_hex != self.private_key_hex else "account"
                priv = PrivateKey(bytes.fromhex(escrow_hex))
                log.info(f"Peach: signing with {key_src} key for {contract_id}")
                if batch_psbt:
                    payload['batchReleaseTransaction'] = sign_psbt(batch_psbt, priv)
                if release_psbt:
                    finalized = build_finalized_tx(release_psbt, priv)
                    if finalized:
                        payload['releaseTransaction'] = finalized
                if payload:
                    log.info(f"Peach: signing release PSBT for {contract_id} ({list(payload.keys())})")
            else:
                log.warning(f"Peach: no PSBT available for {contract_id} at status={trade_status}, sending empty confirm")
        except Exception as e:
            log.error(f"Peach: could not sign PSBT for {contract_id}: {e}")
            raise RuntimeError(f"PSBT signing failed for {contract_id}: {e}") from e

        self._api_call("POST", f"{self.base_url}/contract/{contract_id}/payment/confirm",
                       json=payload, timeout=15)
        try:
            self._api_call("POST", f"{self.base_url}/contract/{contract_id}/rating",
                           json={"rating": 1.0}, timeout=10)
        except Exception:
            pass
        return True

    def release_escrow(self, contract_id, signed_tx_hex):
        """Release escrow by submitting the fully signed transaction"""
        log.info(f"Peach: Releasing escrow for contract {contract_id}...")
        r = self._api_call("POST", f"{self.base_url}/contract/{contract_id}/payment/confirm", json={
            "releaseTransaction": signed_tx_hex
        }, timeout=15)
        data = r.json()
        log.info(f"Peach: Escrow released for {contract_id}")
        return data

    def sign_release_psbt(self, contract_id, escrow_privkey_hex=None):
        """
        Get PSBT from contract and prepare for signing.
        Returns raw data needed for signing (PSBT + key).
        
        Full signing requires bitcoin script library - 
        see the blog post for the finalization code.
        """
        contract = self.get_contract_detail(contract_id)
        psbt_base64 = contract.get("releasePsbt", "")

        if not psbt_base64:
            log.debug(f"Peach: No PSBT available yet for {contract_id}")
            return None

        offer_id = contract.get("offerId", "")
        if not escrow_privkey_hex:
            escrow_privkey_hex = self.escrow_keys.get(offer_id, self.private_key_hex)

        log.info(f"Peach: PSBT ready for signing (contract {contract_id})")
        return {
            "psbt_base64": psbt_base64,
            "escrow_privkey_hex": escrow_privkey_hex,
            "contract": contract
        }

    # ─── MARKET SCANNER ───────────────────────────────────

    def _scan_offers(self, offer_type, currencies=None, payment_methods=None):
        """Shared scan logic with caching for sell/buy offers."""
        if currencies is None:
            currencies = ["CHF", "EUR"]
        if payment_methods is None:
            payment_methods = {
                "CHF": ["twint", "revolut", "wise"],
                "EUR": ["sepa", "instantSepa", "revolut", "wise"],
            }
        # Build cache key from params
        cache_key = f"{offer_type}:{','.join(currencies)}"
        cached = self._scan_cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < self._scan_cache_ttl:
            log.debug(f"Scan cache hit: {cache_key} ({time.time() - cached['ts']:.0f}s old)")
            return cached["data"]

        endpoint = "sellOffer" if offer_type == "sell" else "buyOffer"
        all_offers = {}
        for currency in currencies:
            for method in payment_methods.get(currency, []):
                try:
                    r = self._api_call("GET", f"{self.base_url_v069}/{endpoint}",
                        params={"currency": currency, "paymentMethod": method})
                    data = r.json()
                    raw = data.get("offers", data) if isinstance(data, dict) else data
                    for o in raw:
                        oid = str(o.get("id", ""))
                        if oid and oid not in all_offers:
                            all_offers[oid] = o
                except Exception as e:
                    log.debug(f"Scan {offer_type} {currency}/{method}: {e}")
        result = list(all_offers.values())
        self._scan_cache[cache_key] = {"data": result, "ts": time.time()}
        return result

    def scan_market(self, currencies=None, payment_methods=None):
        """Scan Peach market for competitor sell offers via v069 API (cached 60s)"""
        return self._scan_offers("sell", currencies, payment_methods)

    def scan_buy_offers(self, currencies=None, payment_methods=None):
        """Scan Peach market for buyer demand via v069 API (cached 60s)"""
        return self._scan_offers("buy", currencies, payment_methods)

    # ─── STATUS ────────────────────────────────────────────

    def get_platform_fee_pct(self):
        return 2.0

    def get_status(self):
        try:
            self._ensure_auth()
            r = self.session.get(f"{self.base_url}/user/me", timeout=10)
            r.raise_for_status()
            u = r.json()
            return {
                "name": self.name,
                "escrow_type": self.escrow_type,
                "online": True,
                "trades": u.get("trades", 0),
                "rating": u.get("rating", "?")
            }
        except Exception as e:
            return {"name": self.name, "online": False, "error": str(e)}


# ─── PAYMENT HASH HELPERS ─────────────────────────────────

def make_payment_hash(raw_data: str) -> str:
    """Hash a payment-data FIELD value EXACTLY like the Peach app: sha256(value.toLowerCase()) —
    see peach-app src/utils/paymentMethod/hashPaymentData.ts (verified against their test vector:
    IBAN "IE29 AIBK…" -> 8b703de3…). Pass the RAW single-field value (IBAN / phone / email /
    userName), NOT a JSON blob. Matching Peach's method prevents a buyer-side "payment data could
    not be verified" warning on match.

    Examples:
        Twint:   make_payment_hash("+41790000000")
        SEPA:    make_payment_hash("CH00 0000 0000 0000 0000 0")
        Revolut: make_payment_hash("@user")   # the userName field only, not JSON
    """
    return hashlib.sha256(raw_data.lower().encode()).hexdigest()


def make_payment_data(method: str, raw_data: str) -> dict:
    """Create paymentData dict for offer creation
    
    Returns: {"twint": {"hashes": ["4097dcdb..."]}}
    """
    return {method: {"hashes": [make_payment_hash(raw_data)]}}
