#!/bin/python3
import random
import re
if __name__ == '__main__':
    gen_levels = {}
    filename = "com-youtube.ungraph.txt"
    f_o = open(filename + ".out", "w")
    with open(filename, "r") as f:
        line = "1"
        while len(line) > 0:
            line = f.readline()
            if len(line) <= 0:
                break
            ar = re.split("\t+", line)
            fr = int(ar[0])
            # if fr not in gen_levels:
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
                # gen_levels[fr] = gen_le
            # else:
                # gen_le = gen_levels[fr]
            newline = line.strip("\n") + " " + str(gen_le) + "\n"
            newline = newline.replace("\t", " ")
            f_o.write(newline)
