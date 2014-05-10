import os, sys, re, math, os.path, math
from pylab import *
from draw import *
import matplotlib.pyplot as plt

keys = ["w/o Paxos", "w/ Paxos", "Persistent"]
data = [7.34, 4.56, 4.19]
fig = draw_bar('paxos_persistent.pdf', keys, data)

keys = ["Group Locking", "Per-Row Locking"]
data = [3.79, 7.41]
fig = draw_bar('locking_granularity.pdf', keys, data)

