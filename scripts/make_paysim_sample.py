import csv
import random
import sys

TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]


def main(path: str, rows: int = 1000) -> None:
    rng = random.Random(42)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
                    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"])
        for i in range(rows):
            kind = rng.choice(TYPES)
            amount = round(rng.lognormvariate(8, 1.5), 2)
            old = round(amount + rng.uniform(0, 1e5), 2)
            dest = ("M" if kind == "PAYMENT" else "C") + str(rng.randrange(10**9, 10**10))
            fraud = int(kind in ("TRANSFER", "CASH_OUT") and rng.random() < 0.02)
            w.writerow([1 + i // 100, kind, amount, f"C{rng.randrange(10**9, 10**10)}", old,
                        round(old - amount, 2), dest, 0.0, amount, fraud, 0])


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 1000)
