from dataclasses import dataclass, field
from enum import Enum

class OfferStatus(Enum):
    CREATED = "created"
    FUNDING = "funding"
    FUNDED = "funded"
    MATCHED = "matched"
    PAYMENT_PENDING = "payment_pending"
    PAYMENT_RECEIVED = "payment_received"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DISPUTE = "dispute"

class Platform(Enum):
    PEACH = "peach"

@dataclass
class SellOffer:
    id: str = ""
    platform: Platform = Platform.PEACH
    status: OfferStatus = OfferStatus.CREATED
    premium_pct: float = 0.0
    min_sats: int = 0
    max_sats: int = 0
    escrow_address: str = ""
    payment_methods: list = field(default_factory=list)
    currencies: list = field(default_factory=list)
    created_at: str = ""
    raw_data: dict = field(default_factory=dict)

@dataclass
class Match:
    id: str = ""
    offer_id: str = ""
    platform: Platform = Platform.PEACH
    buyer_id: str = ""
    amount_sats: int = 0
    price_fiat: float = 0.0
    currency: str = "EUR"
    payment_method: str = ""
    raw_data: dict = field(default_factory=dict)

@dataclass
class Contract:
    id: str = ""
    offer_id: str = ""
    platform: Platform = Platform.PEACH
    status: OfferStatus = OfferStatus.MATCHED
    amount_sats: int = 0
    price_fiat: float = 0.0
    currency: str = "EUR"
    payment_method: str = ""
    buyer_id: str = ""
    raw_data: dict = field(default_factory=dict)
