datafile = open('./20151128', 'r')
newfile = open('./20151128.5', 'w+')
x = 0
for tmp in datafile.readlines():
	x = x + 1
	if x > 1000:
		break
	newfile.write(tmp)

datafile.close()
newfile.close()
