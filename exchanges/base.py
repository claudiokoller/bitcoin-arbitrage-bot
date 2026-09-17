from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class BuyResult:
    order_id: str = ""
    btc_amount: float = 0.0
    fiat_spent: float = 0.0
    fee_fiat: float = 0.0
    effective_price: float = 0.0

@dataclass
class WithdrawalResult:
    withdrawal_id: str = ""
    btc_amount: float = 0.0
    fee_btc: float = 0.0
    destination: str = ""
    status: str = ""

class ExchangeBase(ABC):
    name = "base"
    @abstractmethod
    def get_spot_price(self, pair=None): ...
    @abstractmethod
    def get_fiat_balance(self): ...
    @abstractmethod
    def get_btc_balance(self): ...
    @abstractmethod
    def buy_btc_market(self, amount_fiat): ...
    @abstractmethod
    def withdraw_btc(self, address, amount_btc): ...
    def buy_and_withdraw(self, amount_fiat, address):
        buy = self.buy_btc_market(amount_fiat)
        w = self.withdraw_btc(address, buy.btc_amount)
        return {"buy":buy,"withdrawal":w,"total_fiat":buy.fiat_spent,"total_btc":buy.btc_amount,"effective_price":buy.effective_price}
    def get_status(self):
        try:
            return {"name":self.name,"online":True,"fiat_balance":self.get_fiat_balance(),"btc_balance":self.get_btc_balance(),"spot_price":self.get_spot_price()}
        except Exception as e:
            return {"name":self.name,"online":False,"error":str(e)}
