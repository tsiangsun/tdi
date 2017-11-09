import random
random.seed(42)

userids = [x for x in xrange(400)]

for id1 in userids:
  for id2 in userids:
    if id1 < id2 and random.uniform(0., 1.) < .2:
      pair = [id1, id2]
      random.shuffle(pair)
      print "%d\t%d" % tuple(pair)
