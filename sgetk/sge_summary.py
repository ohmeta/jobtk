#!/usr/bin/env python

import sys
import re

# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


def bytes2human(n, format='%(value).1f %(symbol)s', symbols='customary'):
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i+1)*10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()
    return format % dict(symbol=symbols[0], value=n)


def human2bytes(s):
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k' or letter == "m" or letter == "g":
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])


def main():
    count = 0
    count_500G = 0
    count_300G = 0
    count_200G = 0
    count_100G = 0
    count_0G = 0

    total_mem = 0.0
    total_ncor = 0
    total_nthr = 0
    total_mem_used = 0.0
    total_mem_can_use = 0.0

    total_mem_swap = 0.0
    total_mem_swap_used = 0.0

    node_died = set()
    node_danger = set()

    for line in sys.stdin:
        count += 1
        if count == 1:
            print(line.strip() + " MEM_CAN_USE")
        if count > 3:
            line_list = re.split(r'\s+', line.strip())

            mem_total = 0.0
            mem_used = 0.0
            mem_can_use = 0.0
            mem_swap = 0.0
            mem_swap_used = 0.0
            ncor = 0
            nthr = 0

            if line_list[9] != "-" and line_list[9] != "0.0":
                mem_swap = human2bytes(line_list[9])
            else:
                node_died.add(line_list[0])
            if line_list[10] != "-" and line_list[10] != "0.0":
                mem_swap_used = human2bytes(line_list[10])
            else:
                node_died.add(line_list[0])

            if mem_swap / 3 < mem_swap_used:
                node_danger.add(line_list[0])

            if line_list[7] != "-" and line_list[7] != "0.0":
                mem_total = human2bytes(line_list[7])
                mem_tag = mem_total / 1000000000
                if mem_tag >= 500:
                    count_500G += 1
                elif mem_tag >= 300:
                    count_300G += 1
                elif mem_tag >= 200:
                    count_200G += 1
                elif mem_tag >= 100:
                    count_100G += 1
                else:
                    count_0G += 1
            else:
                node_died.add(line_list[0])

            if line_list[8] != '-' and line_list[9] != "0.0":
                mem_used = human2bytes(line_list[8])
            else:
                node_died.add(line_list[0])

            if line_list[4] != "-" and line_list[4] != "0.0":
                ncor = int(line_list[4])
            else:
                node_died.add(line_list[0])
            if line_list[5] != "-" and line_list[5] != "0.0":
                nthr = int(line_list[5])
            else:
                node_died.add(line_list[0])

            total_ncor += ncor
            total_nthr += nthr

            total_mem += mem_total
            total_mem_used += mem_used

            total_mem_swap += mem_swap
            total_mem_swap_used += mem_swap_used

            if line_list[7] != "-" and line_list[8] != "-":
                mem_can_use = mem_total - mem_used
            total_mem_can_use += mem_can_use
            print(line.strip() + " " + bytes2human(mem_can_use))

    print("\nsummary:")
    print("total st.q computer node : %d" % (count - 3))
    print("total st.q computer node (500G ~ ) : %d" % count_500G)
    print("total st.q computer node (300G ~ 500G): %d" % count_300G)
    print("total st.q computer node (200G ~ 300G): %d" % count_200G)
    print("total st.q computer node (100G ~ 200G): %d" % count_100G)
    print("total st.q computer node (0G   ~ 100G): %d" % count_0G)

    print("total st.q cpu cores: %d" % total_ncor)
    print("total st.q cpu threads: %d" % total_nthr)
    print("total st.q memory: %s" % bytes2human(total_mem))
    print("total st.q memory used: %s" % bytes2human(total_mem_used))
    print("total st.q memory can be used: %s" % bytes2human(total_mem_can_use))
    print("total st.q swap memory: %s" % bytes2human(total_mem_swap))
    print("total st.q swap memory used: %s" % bytes2human(total_mem_swap_used))

    print("\ndied st.q node:")
    for i in node_died:
        print(i)

    print("\ndangerous st.q node:")
    for i in node_danger:
        print(i)


if __name__ == "__main__":
    main()