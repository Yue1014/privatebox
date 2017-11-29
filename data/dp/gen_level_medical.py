
import random
def first_index(s):
    for i, c in enumerate(s):
        if c == ",":
            return i
    return -1

if __name__ == '__main__':
    filename="medical.csv"
    f_o = open(filename + ".out", "w")
    with open(filename, 'r') as f:
        line = "1"
        while len(line) > 0:
            line = f.readline()
            if len(line) <= 0:
                break
            gen_le = random.randint(1, 100)
            if gen_le > 99:
                gen_le = 8
            elif gen_le > 95:
                gen_le = 4
            elif gen_le > 80:
                gen_le = 2
            elif gen_le > 50:
                gen_le = 1
            else:
                gen_le = 0
            new_line = str(gen_le) + line[first_index(line):]
            f_o.write(new_line)
