import os, sys, re, math
from pylab import *

def draw_bar(filename, keys, data, ylabel='Throughput (txn/s)'):
	index = range(0, len(data))
	label = keys
	fig, ax1 = plt.subplots(figsize=(4, 3))
	ax1.set_ylabel(ylabel)
	grid(axis='y')
	plt.xticks(index, label)
	width = 0.5
	bars = [0] * len(data)
	plt.xticks([x + 0.25 for x in index], label)
	ax1.set_xlim(-0.2,len(index)-0.3)

	rects1 = ax1.bar([x for x in index], data, width, color=[0.5]*3)
	subplots_adjust(left=0.15, bottom=0.1, right=0.95, top=None)
	savefig('figs/' + filename)

