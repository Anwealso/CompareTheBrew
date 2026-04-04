import threading

class Item:
    # todo expand for alcohol content
    def __init__(
        self,
        store,
        brand,
        name,
        type,
        price,
        link,
        ml,
        percent,
        std_drinks,
        pack_qty,
        score,
        image,
        promotion,
        old_price,
    ):
        self.store = store
        self.brand = brand
        self.name = name
        self.price = price
        self.type = type
        self.link = link
        self.ml = ml
        self.percent = percent
        self.stdDrinks = std_drinks
        self.pack_qty = pack_qty
        self.score = score
        self.image = image
        self.promotion = promotion
        self.old_price = old_price

    def __lt__(self, other):
        return (self.score or float("inf")) < (other.score or float("inf"))

    def __repr__(self):
        # Create a new string
        reprString = ""
        # Add the instance properties to the reprString
        reprString += self.store + ","
        reprString += self.brand + ","
        reprString += self.name + ","
        reprString += self.type + ","
        reprString += str(self.percent) + ","
        reprString += str(self.ml) + ","
        reprString += str(self.pack_qty) + ","
        reprString += str(self.stdDrinks) + ","
        reprString += str(self.price) + ","
        reprString += str(self.score) + ","
        reprString += self.link + ","
        reprString += self.image
        return reprString

class ItemCollection:
    def __init__(self):
        self._lock = threading.Lock()
        self.collection = []
