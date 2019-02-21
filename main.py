from UnionBreak import Breaker


def main():
    b = Breaker("database.db")
    b.columnSplinter("r_split_SpotifySongs_0", 2, 2)


if __name__ == "__main__":
    main()
