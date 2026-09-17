from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

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
    ROBOSATS = "robosats"
    BISQ = "bisq"

class Exchange(Enum):
    KRAKEN = "kraken"
    BINANCE = "binance"
    COINBASE = "coinbase"
    BITSTAMP = "bitstamp"

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

@dataclass
class TradeResult:
    id: str = ""
    platform: Platform = Platform.PEACH
    exchange: Exchange = Exchange.KRAKEN
    timestamp: str = ""
    amount_sats: int = 0
    buy_price_fiat: float = 0.0
    sell_price_fiat: float = 0.0
    currency: str = "EUR"
    premium_pct: float = 0.0
    exchange_fee: float = 0.0
    platform_fee: float = 0.0
    network_fee: float = 0.0
    net_profit: float = 0.0
    payment_method: str = ""
    contract_id: str = ""
    def calculate_profit(self):
        self.net_profit = (self.sell_price_fiat - self.buy_price_fiat - self.exchange_fee - self.platform_fee - self.network_fee)
