def contains(c):
    def func(line):
        return c in line
    return func
