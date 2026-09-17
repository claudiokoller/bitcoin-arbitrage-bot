"""
Taproot (BIP340/341/086) helpers for Peach escrowVersion 2.

Peach's escrowVersion 2 escrow is a key-path-only P2TR output the seller owns
alone: no script tree, no merkle root, no Peach key, no timelock. See
Peach2Peach/peach-app src/utils/wallet/singleSigEscrow.ts. Consequences:

  * the escrow address is a pure function of the seller's escrow pubkey, so it
    can — and must — be verified locally before anything is funded. With the
    legacy 2-of-2 P2WSH escrow a wrong address merely produced an unspendable
    output; here it hands the coins to whoever owns that key.
  * releasing spends the key path, signed with the TWEAKED key and a 64-byte
    BIP340 schnorr signature under SIGHASH_DEFAULT.

No extra dependencies: stdlib + coincurve (which provides schnorr signing and
scalar tweaking). Everything here is deliberately self-contained and covered by
the BIP86 test vector in selftest().
"""
import hashlib
import struct

from coincurve import PrivateKey

# secp256k1 group order
_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_GEN = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
_BECH32M_CONST = 0x2BC830A3  # witness v1+; v0 uses 1

X_ONLY_LEN = 32
COMPRESSED_LEN = 33


def tagged_hash(tag: str, msg: bytes) -> bytes:
    """BIP340 tagged hash: SHA256(SHA256(tag) || SHA256(tag) || msg)."""
    t = hashlib.sha256(tag.encode()).digest()
    return hashlib.sha256(t + t + msg).digest()


def to_x_only(pubkey: bytes) -> bytes:
    """Drop the parity byte from a compressed pubkey. Idempotent on x-only keys."""
    if len(pubkey) == X_ONLY_LEN:
        return pubkey
    if len(pubkey) != COMPRESSED_LEN:
        raise ValueError(f"unexpected pubkey length {len(pubkey)}")
    return pubkey[1:]


# ── bech32m ───────────────────────────────────────────────────────────────────

def _polymod(values):
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= _BECH32_GEN[i] if ((b >> i) & 1) else 0
    return chk


def _hrp_expand(hrp: str):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def _convertbits(data, frombits, tobits, pad=True):
    """Generic base conversion. Unlike the 16-bit-masked copy in hd_keys this
    keeps the full accumulator, so it is safe for any bit width."""
    acc = bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            raise ValueError("invalid value for base conversion")
        acc = (acc << frombits) | value
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        raise ValueError("invalid padding in base conversion")
    return ret


def bech32m_encode(hrp: str, witver: int, witprog: bytes) -> str:
    """Encode a witness v1+ (bech32m) address."""
    if witver == 0:
        raise ValueError("witness v0 uses bech32, not bech32m")
    data = [witver] + _convertbits(witprog, 8, 5)
    values = _hrp_expand(hrp) + data
    const = _BECH32M_CONST
    checksum = [((_polymod(values + [0] * 6) ^ const) >> 5 * (5 - i)) & 31 for i in range(6)]
    return hrp + "1" + "".join(_BECH32_CHARSET[d] for d in data + checksum)


def decode_segwit_address(addr: str):
    """Decode a bech32/bech32m address → (witness_version, program_bytes).

    Validates the checksum against the constant required for that witness
    version, so a v0 address encoded as bech32m (or vice versa) is rejected.
    Raises ValueError on anything malformed.
    """
    if addr != addr.lower() and addr != addr.upper():
        raise ValueError(f"mixed case in address: {addr}")
    a = addr.lower()
    pos = a.rfind("1")
    if pos < 1 or pos + 7 > len(a):
        raise ValueError(f"malformed address: {addr}")
    hrp, body = a[:pos], a[pos + 1:]
    try:
        d = [_BECH32_CHARSET.index(c) for c in body]
    except ValueError:
        raise ValueError(f"invalid character in address: {addr}")
    witver = d[0]
    const = 1 if witver == 0 else _BECH32M_CONST
    if _polymod(_hrp_expand(hrp) + d) != const:
        raise ValueError(f"bad checksum for witness v{witver}: {addr}")
    prog = bytes(_convertbits(d[1:-6], 5, 8, pad=False))
    if witver == 0 and len(prog) not in (20, 32):
        raise ValueError(f"bad v0 program length {len(prog)}")
    if witver == 1 and len(prog) != X_ONLY_LEN:
        raise ValueError(f"bad v1 program length {len(prog)}")
    return witver, prog


# ── P2TR key-path ─────────────────────────────────────────────────────────────

def taproot_tweak_privkey(privkey_bytes: bytes) -> bytes:
    """BIP341 key-path tweak with no script tree.

    d' = (d_even + tagged_hash("TapTweak", x(P))) mod n, where d_even is d
    negated when P has an odd Y — the x-only convention.
    """
    d = int.from_bytes(privkey_bytes, "big")
    if not (0 < d < _N):
        raise ValueError("private key out of range")
    pub = PrivateKey(privkey_bytes).public_key.format(compressed=True)
    if pub[0] == 0x03:  # odd Y → use the negated key so P is even-Y
        d = _N - d
    x_only = pub[1:]
    t = int.from_bytes(tagged_hash("TapTweak", x_only), "big")
    if t >= _N:
        raise ValueError("invalid tweak")
    return ((d + t) % _N).to_bytes(32, "big")


def taproot_output_pubkey(pubkey: bytes) -> bytes:
    """x-only output key Q for a key-path-only P2TR from an internal pubkey."""
    x_only = to_x_only(pubkey)
    t = tagged_hash("TapTweak", x_only)
    # Q = P + t*G, computed on the even-Y form of P
    internal = b"\x02" + x_only
    from coincurve import PublicKey
    q = PublicKey(internal).add(t)
    return q.format(compressed=True)[1:]


def p2tr_script_pubkey(pubkey: bytes) -> bytes:
    """scriptPubKey for a key-path-only P2TR output: OP_1 <32-byte output key>."""
    return bytes([0x51, 0x20]) + taproot_output_pubkey(pubkey)


def p2tr_address(pubkey: bytes, network: str = "mainnet") -> str:
    """bc1p…/tb1p… address for a key-path-only P2TR output."""
    hrp = "bc" if network == "mainnet" else "tb"
    return bech32m_encode(hrp, 1, taproot_output_pubkey(pubkey))


def verify_escrow_address(address: str, pubkey: bytes, network: str = "mainnet") -> bool:
    """Whether `address` is exactly the single-sig escrow for `pubkey`.

    Peach returns the escrow address, but with escrowVersion 2 the seller is the
    sole owner of the key, so an address that does not derive from our own key
    must never be funded. Callers treat False as a hard stop.
    """
    try:
        witver, prog = decode_segwit_address(address)
    except ValueError:
        return False
    return witver == 1 and prog == taproot_output_pubkey(pubkey)


def is_p2tr_address(address: str) -> bool:
    """Whether the address is witness v1 (an escrowVersion 2 escrow).
    Legacy Peach escrows are P2WSH, i.e. witness v0."""
    try:
        witver, _ = decode_segwit_address(address)
    except ValueError:
        return False
    return witver == 1


# ── BIP341 sighash ────────────────────────────────────────────────────────────

SIGHASH_DEFAULT = 0x00
SIGHASH_ALL = 0x01
SIGHASH_NONE = 0x02
SIGHASH_SINGLE = 0x03
SIGHASH_ANYONECANPAY = 0x80


def parse_unsigned_tx(raw: bytes) -> dict:
    """Parse an unsigned (witness-less) transaction.

    Unlike the single-input parser in release_escrow this walks every input and
    output: taproot commits to all of them, so a batch release cannot be signed
    from one input alone.
    """
    o = 0
    version = struct.unpack_from("<I", raw, o)[0]; o += 4
    n_in, o = _read_varint(raw, o)
    inputs = []
    for _ in range(n_in):
        prevout = raw[o:o + 36]; o += 36
        slen, o = _read_varint(raw, o); o += slen  # scriptSig: empty when unsigned
        sequence = struct.unpack_from("<I", raw, o)[0]; o += 4
        inputs.append({"prevout": prevout, "sequence": sequence})
    n_out, o = _read_varint(raw, o)
    outputs_start = o
    outputs = []
    for _ in range(n_out):
        amount = struct.unpack_from("<q", raw, o)[0]; o += 8
        slen, o = _read_varint(raw, o)
        outputs.append((amount, raw[o:o + slen])); o += slen
    outputs_end = o
    locktime = struct.unpack_from("<I", raw, o)[0]
    return {
        "version": version, "locktime": locktime,
        "inputs": inputs, "outputs": outputs,
        "outputs_raw": raw[outputs_start:outputs_end],
    }


def _read_varint(data: bytes, offset: int):
    v = data[offset]
    if v < 0xFD:
        return v, offset + 1
    if v == 0xFD:
        return struct.unpack_from("<H", data, offset + 1)[0], offset + 3
    if v == 0xFE:
        return struct.unpack_from("<I", data, offset + 1)[0], offset + 5
    return struct.unpack_from("<Q", data, offset + 1)[0], offset + 9


def taproot_sighash(tx, input_index: int, prevouts, hash_type: int = SIGHASH_DEFAULT) -> bytes:
    """BIP341 signature hash for a key-path spend without annex.

    `tx` is a dict as produced by parse_unsigned_tx (version, locktime, inputs,
    outputs_raw, outputs). `prevouts` is a list of (amount:int, script:bytes)
    for every input, in order — taproot commits to all of them, which is why a
    taproot signer cannot get away with looking at its own input only.
    """
    if hash_type not in (SIGHASH_DEFAULT, SIGHASH_ALL, SIGHASH_NONE, SIGHASH_SINGLE,
                         SIGHASH_ANYONECANPAY | SIGHASH_ALL,
                         SIGHASH_ANYONECANPAY | SIGHASH_NONE,
                         SIGHASH_ANYONECANPAY | SIGHASH_SINGLE):
        raise ValueError(f"unsupported taproot sighash type {hash_type:#x}")

    base = hash_type & 0x03
    anyone = bool(hash_type & SIGHASH_ANYONECANPAY)
    inputs = tx["inputs"]
    if len(prevouts) != len(inputs):
        raise ValueError("prevouts/inputs length mismatch")

    msg = bytes([hash_type])
    msg += struct.pack("<I", tx["version"])
    msg += struct.pack("<I", tx["locktime"])

    if not anyone:
        msg += hashlib.sha256(b"".join(i["prevout"] for i in inputs)).digest()
        msg += hashlib.sha256(b"".join(struct.pack("<q", a) for a, _ in prevouts)).digest()
        msg += hashlib.sha256(
            b"".join(_varint(len(s)) + s for _, s in prevouts)).digest()
        msg += hashlib.sha256(
            b"".join(struct.pack("<I", i["sequence"]) for i in inputs)).digest()

    if base not in (SIGHASH_NONE, SIGHASH_SINGLE):
        msg += hashlib.sha256(tx["outputs_raw"]).digest()

    spend_type = 0  # key path (ext_flag 0), no annex
    msg += bytes([spend_type])

    if anyone:
        amount, script = prevouts[input_index]
        msg += inputs[input_index]["prevout"]
        msg += struct.pack("<q", amount)
        msg += _varint(len(script)) + script
        msg += struct.pack("<I", inputs[input_index]["sequence"])
    else:
        msg += struct.pack("<I", input_index)

    if base == SIGHASH_SINGLE:
        if input_index >= len(tx["outputs"]):
            raise ValueError("SIGHASH_SINGLE without matching output")
        amount, script = tx["outputs"][input_index]
        msg += hashlib.sha256(
            struct.pack("<q", amount) + _varint(len(script)) + script).digest()

    # 0x00 epoch prefix per BIP341
    return tagged_hash("TapSighash", b"\x00" + msg)


def _varint(n: int) -> bytes:
    if n < 0xFD:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xfd" + struct.pack("<H", n)
    if n <= 0xFFFFFFFF:
        return b"\xfe" + struct.pack("<I", n)
    return b"\xff" + struct.pack("<Q", n)


def sign_taproot_keypath(privkey_bytes: bytes, sighash: bytes,
                         aux: bytes = b"\x00" * 32) -> bytes:
    """64-byte BIP340 schnorr signature over `sighash` with the tweaked key.

    aux defaults to zeros so signing is deterministic: Peach may be asked to
    re-sign the same PSBT after a retry, and an identical signature keeps that
    idempotent.
    """
    tweaked = taproot_tweak_privkey(privkey_bytes)
    sig = PrivateKey(tweaked).sign_schnorr(sighash, aux)
    if len(sig) != 64:
        raise ValueError(f"expected 64-byte schnorr signature, got {len(sig)}")
    return sig


def verify_taproot_signature(sig: bytes, sighash: bytes, pubkey: bytes) -> bool:
    """Verify a key-path signature against the tweaked output key."""
    from coincurve import PublicKeyXOnly
    try:
        return PublicKeyXOnly(taproot_output_pubkey(pubkey)).verify(sig, sighash)
    except Exception:
        return False


# ── selftest ──────────────────────────────────────────────────────────────────

def selftest() -> None:
    """BIP86 test vector — exercises BIP32 derivation, the taproot tweak and
    bech32m in one shot. Raises AssertionError on any mismatch."""
    from core.hd_keys import mnemonic_to_seed, derive_path

    mnemonic = ("abandon abandon abandon abandon abandon abandon "
                "abandon abandon abandon abandon abandon about")
    seed = mnemonic_to_seed(mnemonic)
    expected = {
        "m/86'/0'/0'/0/0": "bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr",
        "m/86'/0'/0'/0/1": "bc1p4qhjn9zdvkux4e44uhx8tc55attvtyu358kutcqkudyccelu0was9fqzwh",
        "m/86'/0'/0'/1/0": "bc1p3qkhfews2uk44qtvauqyr2ttdsw7svhkl9nkm9s9c3x4ax5h60wqwruhk7",
    }
    for path, addr in expected.items():
        priv = derive_path(seed, path)
        pub = PrivateKey(priv).public_key.format(compressed=True)
        got = p2tr_address(pub)
        assert got == addr, f"BIP86 {path}: expected {addr}, got {got}"
        assert verify_escrow_address(addr, pub), f"verify failed for {path}"
        assert is_p2tr_address(addr)
        # scriptPubKey must match the address program
        _, prog = decode_segwit_address(addr)
        assert p2tr_script_pubkey(pub) == bytes([0x51, 0x20]) + prog

    # a bech32 (v0) address must not validate as an escrow, and must decode as v0
    v0 = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
    assert not is_p2tr_address(v0)
    assert decode_segwit_address(v0)[0] == 0

    # tampering with the address must be rejected
    priv = derive_path(seed, "m/86'/0'/0'/0/0")
    pub = PrivateKey(priv).public_key.format(compressed=True)
    assert not verify_escrow_address(expected["m/86'/0'/0'/0/1"], pub)

    # sign/verify round trip
    sighash = hashlib.sha256(b"peach escrow selftest").digest()
    sig = sign_taproot_keypath(priv, sighash)
    assert verify_taproot_signature(sig, sighash, pub), "schnorr round trip failed"
    assert sign_taproot_keypath(priv, sighash) == sig, "signing is not deterministic"


if __name__ == "__main__":
    selftest()
    print("taproot selftest OK")
