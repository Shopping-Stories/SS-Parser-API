from __future__ import annotations
from unicodedata import numeric
from re import match, split
from itertools import chain

# Class to make dealing with money easier, implements arithmetic with money as well as dict-style accessing of attributes
# e.g. if type(a) is Money, a["pounds"] == a["Pounds"] == a["L"] is the amount of pounds in the transaction
class Money:
    def __init__(self, moneystr=None, l=0, s=0, d=0, f=0, tot_f=None, context=None) -> None:
        
        farthings = 0
        pounds = 0
        shillings = 0
        pennies = 0
        self.totalFarthings = 0

        if tot_f is not None:
            self.totalFarthings = tot_f
        else:
            if moneystr is not None:
                if match(r"\d+[Lsdp]", moneystr.strip()):
                    moneystr = moneystr.split(" ")
                    unit = moneystr[0][-1]
                    money = moneystr[0][:-1]
                    if unit == "L":
                        pounds = int(money)
                        if len(moneystr) > 1:
                            shillings = int(numeric(moneystr[1]) * 20)
                    elif unit == "s":
                        shillings = int(money)
                        if len(moneystr) > 1:
                            pennies = int(numeric(moneystr[1]) * 12)
                    elif unit in "dp":
                        pennies = int(money)
                        if len(moneystr) > 1:
                            farthings = int(numeric(moneystr[1]) * 4)
                    else:
                        raise ValueError(f"This error should never be raised, panic if it is. It was raised by this: {moneystr} in context {context}. We think unit is {unit}.")
                    

                elif match(r"((\:|(\d+))\/)?(\:|(\d+))\/(\:|(\d+))", moneystr.strip()):
                    moneystr = moneystr.split("/")
                    if len(moneystr) == 2:
                        if len(doublesplit := split(r"\s+", moneystr[1])) > 1:
                            if len(doublesplit) > 2:
                                raise ValueError(f"Bad money string {moneystr}.")
                            try:
                                if numeric(doublesplit[1]) < 1 and numeric(doublesplit[1]) > 0:
                                    s, crap = moneystr
                                    p = doublesplit[0]
                                    f = round(numeric(doublesplit[1]) * 4)
                                    if s != ":":
                                        shillings = int(s)
                                    if p != ":":
                                        pennies = int(p)
                                    farthings = f
                                else:
                                    raise ValueError(f"Bad money string {moneystr}.")
                            except TypeError:
                                raise ValueError(f"Bad money string {moneystr}")
                        else:
                            s, p = moneystr
                            if s != ":":
                                shillings = int(s)
                            if p != ":":
                                pennies = int(p)

                    elif len(moneystr) == 3:
                        l, s, p = moneystr

                        if l != ":":
                            pounds = int(l)
                        if s != ":":
                            shillings = int(s)
                        if p != ":":
                            pennies = int(p)
                    
                    else:
                        raise ValueError(f"Money string {moneystr} was too long for the l/s/d format")

                else:
                    raise ValueError(f"Money string {moneystr} did not match any known patterns in context {context}")
            elif type(l) is int and type(s) is int and type(d) is int and type(f) is int:
                pounds = l
                shillings = s
                pennies = d
                farthings = f
            else:
                # Handle nasty formats
                if f != 0:
                    raise ValueError(f"You cannot give Money nasty {l=}, {s=}, and {d=} and also provide {f=} in context {context}.")
                if "/" in l or "/" in "s" or "/" in d:
                    raise ValueError(f"You cannot provide a mixed / format to Money in context {context}.")
                if type(l) is str and l in ("-", ""):
                    l = 0
                if type(s) is str and s in ("-", ""):
                    s = 0
                if type(d) is str and d in ("-", ""):
                    d = 0
                pounds = l
                if type(l) is str:
                    pounds = int(l.strip("[]"))
                shillings = s
                if type(s) is str:
                    shillings = int(s.strip("[]"))
                pennies = d
                if type(d) is str:
                    if " " in d:
                        d = d.split(" ")
                        if len(d) != 2:
                            raise ValueError(f"Value of {d=} badly formatted for money in context {context}")
                        try:
                            pennies = int(d[0])
                            farthings = int(4 * numeric(d[1]))
                        except TypeError:
                            raise ValueError(f"Value of {d=} badly formatted for money in context {context}.")
                    else:
                        pennies = int(d.strip("[]"))

            self.totalFarthings = farthings + 4 * pennies + 4 * 12 * shillings + 4 * 12 * 20 * pounds

    def __add__(self, other):
        if type(other) is not Money and other == 0:
            return self
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        return Money(tot_f= other.totalFarthings + self.totalFarthings)
    
    def __radd__(self, other):
        if type(other) is not Money and other == 0:
            return self
        else:
            return self.__add__(other)

    def __truediv__(self, other):
        if type(other) is Money:
            return self.totalFarthings / other.totalFarthings
        else:
            return Money(tot_f=self.totalFarthings / other)
    
    def __floordiv__(self, other):
        return int(self.__truediv__(self, other))

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        l, s, d, f = self.get_lsdf()
        return f"Pounds: {l}, Shillings: {s}, Pennies: {d}, Farthings: {f}"

    def __getattr__(self, key: str):
        if "pound" in key.lower() or key.lower() == "l":
            return self.get_lsdf()[0]
        elif "shilling" in key.lower() or key.lower() == "s":
            return self.get_lsdf()[1]
        elif key.lower() in ["pennies", "penny", "pence", "p", "d"]:
            return self.get_lsdf()[2]
        elif "farthing" in key.lower() or key.lower() == "f":
            return self.get_lsdf()[3]
        else:
            raise KeyError(f"Error: Key {key} is an invalid money term")
 
    def __getitem__(self, key: str):
        return self.__getattr__(key)

    def __sub__(self, other):
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        return Money(tot_f= self.totalFarthings - other.totalFarthings)

    def __mul__(self, other: float):
        if type(other) is Money:
            raise ValueError("Erorr, cannot multiply money by money.")
        else:
            return Money(tot_f=self.totalFarthings * other)

    def __iadd__(self, other: Money):
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        self.totalFarthings += other.totalFarthings
        return self

    def __isub__(self, other: Money):
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        self.totalFarthings -= other.totalFarthings
        return self

    def __imul__(self, other):
        if type(other) is Money:
            raise ValueError(f"Error: Cannot multiply money by money")
        self.totalFarthings *= other
        self.totalFarthings = round(self.totalFarthings)
        return self

    def __eq__(self, other: Money) -> bool:
        if type(other) is not Money:
            if other == 0:
                return 0 == self.totalFarthings
            raise ValueError(f"Error: other {other} must be of type money")
        return self.totalFarthings == other.totalFarthings

    def __lt__(self, other: Money) -> bool:
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        return self.totalFarthings < other.totalFarthings

    def __le__(self, other: Money) -> bool:
        if type(other) is not Money:
            raise ValueError(f"Error: other {other} must be of type money")
        return self.totalFarthings <= other.totalFarthings

    def get_lsdf(self):
        tot_f = abs(self.totalFarthings)
        l = tot_f // (4 * 12 * 20)
        tot_f -= l * 4 * 12 * 20
        s = tot_f // (4 * 12)
        tot_f -= s * 4 * 12
        d = tot_f // (4)
        tot_f -= d * 4
        f = int(tot_f)
        if self.totalFarthings < 0:
            return (-l, -s, -d, -f)
        else:
            return (l, s, d, f)