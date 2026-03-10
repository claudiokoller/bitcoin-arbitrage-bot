"""
Peach Bitcoin P2P Platform Integration.

Handles the full seller lifecycle on Peach's P2P marketplace:
  - Authentication via secp256k1 signature
  - Sell offer creation with payment method hashes
  - HD-derived escrow key management (BIP32)
  - Trade request detection and acceptance (v069 API)
  - Payment data encryption (PGP symmetric key exchange)
  - Contract monitoring and PSBT release signing
  - Live premium updates via PATCH API
  - Market scanning for competitive analysis
"""
import hashlib, json, logging, time
from typing import Optional
import requests
from platforms.base import PlatformBase
from core.models import SellOffer, Match, Contract, OfferStatus, Platform
log = logging.getLogger("bot.peach")


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
        self.payment_data_raw = config.get("payment_data_raw", {})
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.access_token = None
        self._auth_time = 0
        self.escrow_keys = {}

    def _get_escrow_privkey_hex(self, offer_id) -> str:
        """Derive per-offer escrow key: m/84'/0'/0'/{offerId}' (matching Peach app)."""
        if self.mnemonic:
            from core.hd_keys import get_peach_escrow_privkey
            return get_peach_escrow_privkey(self.mnemonic, offer_id, self.network)
        return self.escrow_keys.get(str(offer_id), self.private_key_hex)

    # --- AUTH ---

    def authenticate(self):
        """Authenticate with Peach API using secp256k1 signature."""
        from coincurve import PrivateKey
        privkey = PrivateKey(bytes.fromhex(self.private_key_hex))
        pubkey_hex = privkey.public_key.format(compressed=True).hex()

        ts = str(int(time.time() * 1000))
        message = f"Peach Registration {ts}"
        msg_hash = hashlib.sha256(message.encode()).digest()
        signature = privkey.sign_recoverable(msg_hash, hasher=None)[:64].hex()

        r = self.session.post(f"{self.base_url}/user/auth", json={
            "publicKey": pubkey_hex, "uniqueId": self.unique_id,
            "message": message, "signature": signature
        }, timeout=15)
        r.raise_for_status()
        self.access_token = r.json().get("accessToken")
        self.session.headers["Authorization"] = f"Bearer {self.access_token}"
        self._auth_time = time.time()
        log.info("Peach auth OK")
        return True

    def _ensure_auth(self):
        if not self.access_token or (time.time() - self._auth_time > 3000):
            self.authenticate()

    def _api_call(self, method, url, **kwargs):
        """API call with automatic 401 retry and 5xx backoff."""
        self._ensure_auth()
        kwargs.setdefault("timeout", 15)
        r = self.session.request(method, url, **kwargs)
        if r.status_code == 401:
            self.authenticate()
            r = self.session.request(method, url, **kwargs)
        if r.status_code >= 500:
            time.sleep(2)
            r = self.session.request(method, url, **kwargs)
        r.raise_for_status()
        return r

    # --- OFFERS (v1) ---

    def create_sell_offer(self, min_sats, max_sats, premium_pct, payment_methods, **kw):
        """Create a sell offer with payment data hashes.

        Payment data hashing varies by method:
        - Twint/SEPA: sha256(raw_string)
        - Revolut/Wise: sha256(json.dumps({"userName": ..., "reference": ""}))
        """
        self._ensure_auth()
        payload = {
            "type": "ask",
            "amount": max_sats,
            "premium": premium_pct,
            "meansOfPayment": payment_methods,
            "returnAddress": self.refund_address
        }
        if self.pgp_public_key:
            payload["pgpPublicKey"] = self.pgp_public_key

        # Build payment hashes from configured raw data
        payment_data = self._build_payment_hashes()
        if payment_data:
            # Filter meansOfPayment to methods with valid hashes
            filtered_mop = {}
            for currency, methods in payload["meansOfPayment"].items():
                valid = [m for m in methods if m in payment_data]
                if valid:
                    filtered_mop[currency] = valid
            if filtered_mop:
                payload["meansOfPayment"] = filtered_mop
            payload["paymentData"] = payment_data

        r = self._api_call("POST", f"{self.base_url}/offer", json=payload)
        data = r.json()
        oid = data.get("offerId", data.get("id", ""))
        esc = data.get("escrow", {})
        return SellOffer(
            id=oid, platform=Platform.PEACH, status=OfferStatus.CREATED,
            premium_pct=premium_pct, min_sats=min_sats, max_sats=max_sats,
            escrow_address=esc.get("address", "") if esc else "",
            created_at=data.get("createdAt", ""), raw_data=data
        )

    def _build_payment_hashes(self):
        """Build payment data hashes from config. Implementation omitted for security."""
        raise NotImplementedError("Payment hash construction - see private repo")

    def create_escrow(self, offer_id, public_key_hex=None):
        """Create escrow with HD-derived per-offer key."""
        if not public_key_hex:
            from coincurve import PrivateKey
            escrow_hex = self._get_escrow_privkey_hex(offer_id)
            escrow_privkey = PrivateKey(bytes.fromhex(escrow_hex))
            public_key_hex = escrow_privkey.public_key.format(compressed=True).hex()
            self.escrow_keys[str(offer_id)] = escrow_hex

        r = self._api_call("POST", f"{self.base_url}/offer/{offer_id}/escrow",
            json={"publicKey": public_key_hex})
        data = r.json()

        escrow_addr = ""
        if "escrows" in data:
            escrow_addr = data["escrows"].get("bitcoin", "")
        elif "escrow" in data:
            escrow_addr = data["escrow"] if isinstance(data["escrow"], str) \
                else data["escrow"].get("address", "")

        return {"address": escrow_addr, "raw": data}

    def get_escrow_address(self, offer_id):
        r = self._api_call("GET", f"{self.base_url}/offer/{offer_id}/escrow")
        data = r.json()
        return data.get("address", data.get("escrows", {}).get("bitcoin", ""))

    def get_escrow_status(self, offer_id):
        r = self._api_call("GET", f"{self.base_url}/offer/{offer_id}/escrow")
        return r.json()

    def get_active_offers(self):
        r = self._api_call("GET", f"{self.base_url}/offers")
        offers = []
        status_map = {
            "active": OfferStatus.FUNDED, "fundEscrow": OfferStatus.FUNDING,
            "matched": OfferStatus.MATCHED, "searchingForPeer": OfferStatus.CREATED,
        }
        for o in r.json():
            if o.get("type") != "ask":
                continue
            offers.append(SellOffer(
                id=o.get("id", o.get("offerId", "")),
                platform=Platform.PEACH,
                status=status_map.get(o.get("status", ""), OfferStatus.CREATED),
                premium_pct=o.get("premium", 0),
                min_sats=o.get("amount", [0, 0])[0] if isinstance(o.get("amount"), list) else 0,
                max_sats=o.get("amount", [0, 0])[1] if isinstance(o.get("amount"), list) else 0,
                raw_data=o
            ))
        return offers

    def cancel_offer(self, offer_id):
        self._api_call("POST", f"{self.base_url}/offer/{offer_id}/cancel")
        return True

    def update_premium(self, offer_id, new_premium):
        """PATCH live offer premium. Only works on online=True offers."""
        try:
            self._api_call("PATCH", f"{self.base_url}/offer/{offer_id}",
                json={"premium": new_premium})
            log.info(f"Peach: updated {offer_id} premium to {new_premium}%")
            return True
        except Exception as e:
            log.warning(f"Peach: PATCH {offer_id}: {e}")
            return False

    # --- TRADE REQUESTS (v069) ---

    def check_trade_requests(self, offer_id):
        """Check for incoming trade requests using v069 API.
        v069 is the working alternative to the often-empty v1 matches endpoint."""
        url = f"{self.base_url_v069}/sellOffer/{offer_id}/tradeRequestReceived"
        r = self._api_call("GET", url)
        data = r.json()
        requests_list = data if isinstance(data, list) else data.get("matches", [])
        if requests_list:
            log.info(f"Peach: {len(requests_list)} trade request(s) for {offer_id}")
        return requests_list

    def accept_trade_request(self, offer_id, buyer_user_id, payment_method,
                              payment_data_raw, trade_request_data=None):
        """Accept trade request with encrypted payment data.

        Encryption flow:
        1. Buyer sends symmetricKeyEncrypted (AES key encrypted with our PGP key)
        2. We decrypt AES key with our PGP private key
        3. Encrypt payment data with AES key (PGP symmetric)
        4. Sign symmetric key with our PGP private key

        Implementation omitted for security.
        """
        raise NotImplementedError("Trade acceptance with PGP encryption - see private repo")

    # --- CONTRACTS ---

    def get_contracts(self):
        r = self._api_call("GET", f"{self.base_url}/contracts/summary", timeout=10)
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
            # Summary endpoint lacks paymentMethod - fetch from detail if missing
            if not payment_method and contract_id:
                try:
                    detail = self._api_call("GET", f"{self.base_url}/contract/{contract_id}", timeout=10)
                    d = detail.json()
                    payment_method = d.get("paymentMethod", "")
                except Exception:
                    pass
            status_str = c.get("tradeStatus", c.get("status", ""))
            contracts.append(Contract(
                id=contract_id,
                offer_id=c.get("offerId", ""),
                platform=Platform.PEACH,
                status=sm.get(status_str, OfferStatus.MATCHED),
                amount_sats=c.get("amount", 0),
                price_fiat=c.get("price", 0),
                currency=c.get("currency", "EUR"),
                payment_method=payment_method,
                raw_data=c
            ))
        return contracts

    def confirm_payment(self, contract_id):
        """Confirm payment and sign release PSBT.

        PSBT signing requires the correct escrow key (HD-derived or account key).
        Auto-detects correct key from PSBT witness_script.
        Implementation omitted - see release_escrow.py.
        """
        raise NotImplementedError("PSBT signing - see private repo")

    # --- MARKET SCANNER ---

    def scan_market(self, currencies=None, payment_methods=None):
        """Scan Peach market for competitor sell offers via v069 API."""
        if currencies is None:
            currencies = ["CHF", "EUR"]
        if payment_methods is None:
            payment_methods = {
                "CHF": ["twint", "revolut", "wise"],
                "EUR": ["sepa", "instantSepa", "revolut", "wise"],
            }
        all_offers = {}
        for currency in currencies:
            for method in payment_methods.get(currency, []):
                try:
                    r = self._api_call("GET", f"{self.base_url_v069}/sellOffer",
                        params={"currency": currency, "paymentMethod": method})
                    data = r.json()
                    raw = data.get("offers", data) if isinstance(data, dict) else data
                    for o in raw:
                        oid = str(o.get("id", ""))
                        if oid and oid not in all_offers:
                            all_offers[oid] = o
                except Exception as e:
                    log.debug(f"Scan {currency}/{method}: {e}")
        return list(all_offers.values())

    def get_platform_fee_pct(self):
        return 2.0  # Peach charges 2% to the buyer


# --- PAYMENT HASH HELPERS ---

def make_payment_hash(raw_data: str) -> str:
    """SHA256 hash of raw payment data string.

    Examples:
        Twint:    sha256("+41XXXXXXXXX")
        SEPA:     sha256("CHXXXXXXXXXXXX")
        Revolut:  sha256(json.dumps({"userName": "@user", "reference": ""}))
    """
    return hashlib.sha256(raw_data.encode()).hexdigest()
