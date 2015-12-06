import sys
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import gui

# Filter criteria - change it to accept from UI
print "starting"
gui.getValues()
country = gui.country
print country
indicator = gui.indicator
print indicator
year_start = gui.startYear
print year_start
year_end = gui.endYear
print  year_end
num_of_segments = 5

predict_end = gui.predYear  # predict till year



conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("damu1000")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

# read data
lines = sc.pickleFile(".//result")
# filter by country, indicator and period. sort by period
lines = lines.filter(lambda x: x[0] == country and x[2] == indicator and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a, b, c, d, e): d, True).cache()
lines.take(1)
if not lines.take(1):
    print "Index not present for this country. Stopping"
    sys.exit()
print lines.collect()
x = lines.map(lambda (a, b, c, d, e): (d))  # getting x values in 2D RDD
y = lines.map(lambda (a, b, c, d, e): float(e))  # getting x values in 2D RDD


# num_of_segments = x.count() / 5 #averaging at around 5 points per segment
#--------------------------------------- Find out "break" points in pattern--------------------------------------------------------------------

# assign indexes to y values, increment by 1 and 2 so that elements can be joined to find out diff later.
x0 = x.zipWithIndex().map(lambda (a, b): (b, a))
y0 = y.zipWithIndex().map(lambda (a, b): (b, a))
y1 = y0.map(lambda (a, b): (a + 1, b))
y2 = y0.map(lambda (a, b): (a + 2, b))
# join structure is like: [index, ((y0, y1) ,y2 )]. Hence the structure used in next map
# y[1] -> y[i+2], y[0][1] -> y[i+1], y[0][0] -> y[i]. Calculating diff (y[i+2]-y[i+1]) - (y[i+1]-y[i]) to find out breakpoint
# finding out difference in y coordinates of every consecutive pair. sort by desc order of difference
y_join = y0.join(y1).join(y2).map(lambda (x, y): (x, abs((y[1] - y[0][1]) - (y[0][1] - y[0][0])))).sortBy(lambda (a, b): b, False)

# caution: segments are derived from zip index. hence contain index of element NOT x element (i.e. year)
# picking up top difference elements - they form our segments
segments = sorted(y_join.map(lambda (a, b):a).take(num_of_segments))  # taking 2 additional segments. Because some times diff returns 2 consecutive points, which has to be discarded
segments = [0] + segments
segments.append(x.count())
print segments
i = 0
# remove consecutive points if included
# check for divide by 0. some prob in segmentation. print x1, check segment should have more than one element.
while i < len(segments) - 1:
        if segments[i] + 1 == segments[i + 1] or segments[i] + 2 == segments[i + 1]:
                segments.remove(segments[i])
	else:
	        i = i + 1  # increment i only in else, cause if element is deleted, then next element is now at same old i th position
if segments[0] != 0:
	segments = [0] + segments

print segments

#------------------------------------------------Calculate linear regression for every segment---------------------------------------------
seg = 0
y_est = []
y = y.collect()

# for carry out linear regression for individual segment
while seg < len(segments) - 1:
	print "----------------------------------------------------------------------------------------------------"
	start = segments[seg]
	end = segments[seg + 1]
        # B1 = sum( (y_avg - yi)(x_avg - xi) ) / sum ( (x_avg - xi)(x_avg - xi) )
        # B0 = y_avg - B1*x_avg
	x1 = x0.filter(lambda (a, b): a >= start and a < end).map(lambda (a, b):float(b))  # filter x and y for current segment start and end
	y1 = y0.filter(lambda (a, b): a >= start and a < end).map(lambda (a, b):float(b))
	seg = seg + 1

        x_avg = float(x1.sum()) / x1.count()  # find out average
        y_avg = float(y1.sum()) / y1.count()
        i = 0
        num = 0
        den = 0
	
	print "x1: ", x1.collect()
	print "y1: ", y1.collect()
	
	num = x1.zip(y1).map(lambda (a, b): (y_avg - b) * (x_avg - a)).sum()  # numerator and denominator as per above formula of linear regression
	den = den + x1.map(lambda a: (x_avg - a) * (x_avg - a)).sum()

        B1 = num / den  # B0 and B1 (Beta 0 and Beta 1) calculation
        B0 = y_avg - B1 * x_avg

	# y1 = y1.collect()
	y_est = y_est + x1.map(lambda a: B0 + B1 * a).collect()  # calculate estimated linear regression using Beta0 and Beta 1 (finally!!!)

#---------------------------------------extend x with upcoming years and predict using last segment's B0 and B1
print "estimating--------------------------------------------------------------------------------------"
x = x.collect()
predict_start = x[len(x) - 1] + 1  # first year of prediction = last year of available data + 1

x_est = x + range(predict_start, predict_end + 1)  # adding + 1 to accomodate end year as well

for i in range(predict_start, predict_end + 1):
	y_est.append(B0 + B1 * i)

# plt.plot(x,y,'bs', x_est, y_est, 'g--')
# plt.show()
print "plotting"
print "x_est: ", x_est
print len(x_est)
print "y_est: ", y_est
print len(y_est)

plt.xlabel('Year')
plt.ylabel(indicator)

line1, = plt.plot(x_est, y_est, linestyle="solid", marker='o', color="blue")
line2, = plt.plot(x, y, linestyle="s", marker="o", color="red")

plt.legend([line1, line2], ["Prediction", "Real Data"])
plt.title("Prediction using Segmented Linear Regression: " + country)
plt.show()


