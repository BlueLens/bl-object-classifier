from collections import Counter

l=['1', '1', '1', '1', '1', '1', '2', '2', '2', '2', '7', '7', '7', '10', '10']
c=Counter(l)
k = c.most_common()
print(k[0][0])
