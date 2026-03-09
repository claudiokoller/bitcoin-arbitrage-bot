"""
BIP32/BIP39 HD key derivation for Peach escrow keys.
No extra dependencies - uses only stdlib + coincurve (already installed).

Peach derivation paths:
  Account key (auth):  m/48'/0'/0'/0'   (mainnet) / m/48'/1'/0'/0'  (testnet)
  Escrow key (current): m/84'/0'/0'/{offerId}'
  Escrow key (legacy):  m/48'/0'/0'/{offerId}'
  Wallet receive:       m/84'/0'/0'/0/i  (BIP84 standard)
  Wallet change:        m/84'/0'/0'/1/i  (BIP84 standard)
"""
import hashlib, hmac, struct


# secp256k1 curve order
_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

# bech32 charset
_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]


def mnemonic_to_seed(mnemonic: str, passphrase: str = "") -> bytes:
    """BIP39: convert mnemonic phrase to 64-byte seed."""
    return hashlib.pbkdf2_hmac(
        "sha512",
        mnemonic.strip().encode("utf-8"),
        ("mnemonic" + passphrase).encode("utf-8"),
        2048,
    )


def _master_key(seed: bytes):
    """BIP32: seed -> (privkey_bytes, chaincode_bytes)."""
    I = hmac.new(b"Bitcoin seed", seed, hashlib.sha512).digest()
    return I[:32], I[32:]


def _child_hardened(parent_key: bytes, parent_chain: bytes, index: int):
    """BIP32 hardened child derivation. index is the plain child number (< 2^31)."""
    i = 0x80000000 + index
    data = b"\x00" + parent_key + struct.pack(">I", i)
    I = hmac.new(parent_chain, data, hashlib.sha512).digest()
    IL, IR = I[:32], I[32:]
    child_int = (int.from_bytes(IL, "big") + int.from_bytes(parent_key, "big")) % _N
    return child_int.to_bytes(32, "big"), IR


def _child_normal(parent_key: bytes, parent_chain: bytes, index: int):
    """BIP32 normal (non-hardened) child derivation. index < 2^31."""
    from coincurve import PrivateKey as _PK
    parent_pubkey = _PK(parent_key).public_key.format(compressed=True)
    data = parent_pubkey + struct.pack(">I", index)
    I = hmac.new(parent_chain, data, hashlib.sha512).digest()
    IL, IR = I[:32], I[32:]
    child_int = (int.from_bytes(IL, "big") + int.from_bytes(parent_key, "big")) % _N
    return child_int.to_bytes(32, "big"), IR


def derive_path(seed: bytes, path: str) -> bytes:
    """Derive private key at BIP32 path. Supports hardened (') and normal components.
    e.g. "m/84'/0'/0'/0/0" or "m/84'/0'/0'/12345'"
    Returns 32-byte private key."""
    privkey, chaincode = _master_key(seed)
    for part in path.split("/")[1:]:
        hardened = part.endswith("'")
        index = int(part.rstrip("'"))
        if hardened:
            privkey, chaincode = _child_hardened(privkey, chaincode, index)
        else:
            privkey, chaincode = _child_normal(privkey, chaincode, index)
    return privkey


# -- Bitcoin address helpers --

def _bech32_polymod(values):
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= _BECH32_GEN[i] if ((b >> i) & 1) else 0
    return chk


def _bech32_hrp_expand(hrp):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def _convertbits(data, frombits, tobits, pad=True):
    acc = bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    for value in data:
        acc = ((acc << frombits) | value) & 0xFFFF
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad and bits:
        ret.append((acc << (tobits - bits)) & maxv)
    return ret


def bech32_encode_segwit(hrp: str, witver: int, witprog: bytes) -> str:
    """Encode a native SegWit address (P2WPKH or P2WSH)."""
    data = [witver] + _convertbits(witprog, 8, 5)
    values = _bech32_hrp_expand(hrp) + data
    checksum = [((_bech32_polymod(values + [0]*6) ^ 1) >> 5*(5-i)) & 31 for i in range(6)]
    return hrp + "1" + "".join(_BECH32_CHARSET[d] for d in data + checksum)


def p2wpkh_address(pubkey_bytes: bytes, network: str = "mainnet") -> str:
    """Compute P2WPKH bech32 address from compressed public key (33 bytes)."""
    h160 = hashlib.new("ripemd160", hashlib.sha256(pubkey_bytes).digest()).digest()
    return bech32_encode_segwit("bc" if network == "mainnet" else "tb", 0, h160)


def get_peach_account_privkey(mnemonic: str, network: str = "mainnet") -> str:
    """Peach account key: m/48'/0'/0'/0' (mainnet) / m/48'/1'/0'/0' (testnet)."""
    coin = 0 if network == "mainnet" else 1
    seed = mnemonic_to_seed(mnemonic)
    return derive_path(seed, f"m/48'/{coin}'/0'/0'").hex()


def get_peach_escrow_privkey(mnemonic: str, offer_id, network: str = "mainnet",
                              legacy: bool = False) -> str:
    """Peach escrow key for a given offer.
    Current: m/84'/0'/0'/{offer_id}'
    Legacy:  m/48'/0'/0'/{offer_id}'"""
    coin = 0 if network == "mainnet" else 1
    numeric_id = int(str(offer_id).split("-")[0])
    purpose = 48 if legacy else 84
    seed = mnemonic_to_seed(mnemonic)
    return derive_path(seed, f"m/{purpose}'/{coin}'/0'/{numeric_id}'").hex()


def verify_account_key(mnemonic: str, expected_privkey_hex: str,
                        network: str = "mainnet") -> bool:
    """Verify mnemonic produces expected account key."""
    derived = get_peach_account_privkey(mnemonic, network)
    return derived == expected_privkey_hex.lower()
