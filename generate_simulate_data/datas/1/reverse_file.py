f = open("partition_inf", "r+", encoding="utf-8")
origin = []
for line in f:
    origin.append(line)
f.close()

f = open("partition_inf", "w+", encoding="utf-8")
for line in reversed(origin):
    f.write(line)
f.close()