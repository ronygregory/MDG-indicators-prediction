import sys
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
from math import sqrt, log, pow, e, pi
import numpy as np

def sign(x):	#function to return sign of number.
	if x > 0:
		return 1
	elif x < 0:
		return -1
	else:
		return 0

def std_dev(a):	#a will be array of form ((((8.22, 11.93), 15.49), 20.63), 43.73). Repeat each ele 20 times to for array of 100 elements to represent %.
	#print "inside std_dev: ", a
	ele=[]
	temp=[]
	temp.append(0)	#dummy
	temp[0] = a[0][0][0][0]/20
	ele = ele + [temp[0]]*20
	temp[0] = a[0][0][0][1]/20
	ele = ele + [temp[0]]*20
	temp[0] = a[0][0][1]/20
	ele = ele + [temp[0]]*20
	temp[0] = a[0][1]/20
	ele = ele + [temp[0]]*20
	temp[0] = a[1]/20
	ele = ele + [temp[0]]*20
	#print "elements array: " ,ele
	avg = sum(ele) / len(ele)
	tot = 0
	for i in ele:
		tot = tot + (i-avg)*(i-avg)
	dev = sqrt(tot/100)
	return(dev)


#Filter criteria - change it to accept from UI
print "starting"
country="United States"
predict_end=2025	#predict till year
z = 2*365	#z in formula is poverty line.
GINI=0.54115

indicator="Adjusted net national income per capita (constant 2005 US$)"	#this is yt in formula
year_start = 1971
year_end = 2015
num_of_segments = 5

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("damu1000")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

#read data
lines = sc.pickleFile(".//result").filter(lambda x: x[0]==country).cache()
#filter by country, indicator and period. sort by period



#-----------------------------------------read 1st to 5th 20% income----------------------------------------------------------
#using inequality data to approximate standard deviation
income_20_1 = lines.filter(lambda x: x[0]==country and x[2]=="Income share held by lowest 20%" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True)
years = income_20_1.map(lambda (a,b,c,d,e): float(d)  )	#years for which income share data is not null. use this later to filter average income
income_20_1 = income_20_1.map(lambda (a,b,c,d,e): float(e)  )
income_20_2 = lines.filter(lambda x: x[0]==country and x[2]=="Income share held by second 20%" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True).map(lambda (a,b,c,d,e): float(e)  )
income_20_3 = lines.filter(lambda x: x[0]==country and x[2]=="Income share held by third 20%" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True).map(lambda (a,b,c,d,e): float(e)  )
income_20_4 = lines.filter(lambda x: x[0]==country and x[2]=="Income share held by fourth 20%" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True).map(lambda (a,b,c,d,e): float(e)  )
income_20_5 = lines.filter(lambda x: x[0]==country and x[2]=="Income share held by highest 20%" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True).map(lambda (a,b,c,d,e): float(e)  )

PPP = lines.filter(lambda x: x[0]==country and x[2]=="Poverty headcount ratio at $2 a day (PPP) (% of population)" and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True)

PPP_YEAR = PPP.map(lambda (a,b,c,d,e): (d)).collect()
PPP = PPP.map(lambda (a,b,c,d,e): float(e)).collect()

#ZIP will create data structure of format ((((8.22, 11.93), 15.49), 20.63), 43.73). converting to (8.22, 11.93, 15.49, 20.63, 43.73)
inequality = income_20_1.zip(income_20_2).zip(income_20_3).zip(income_20_4).zip(income_20_5)
#.map(lambda (a,b): (a[0][0][0][0],a[0][0][0][1] ,a[0][0][1] ,a[0][1] , b)  )
#print "inquality b4 std deviation: ", inequality.collect()
inequality = inequality.map(std_dev).collect()

#print "inequality: ", inequality

# read income per capita
lines = lines.filter(lambda x: x[0]==country and x[2]==indicator and x[4] != '' and x[3] >= year_start and x[3] <= year_end).sortBy(lambda (a,b,c,d,e): d, True)

x = lines.map(lambda (a,b,c,d,e): (d)  )	#getting x values in 2D RDD
y = lines.map(lambda (a,b,c,d,e): float(e)  )	#getting x values in 2D RDD

#interpolate ineuality data for all the years.
inequality = list(np.interp(x.collect(),years.collect(),inequality))	#using seperate rdd to keep array for later use
#print "x: ", x.collect()
#print "inequality after interpolation: ", inequality
#print x.count()
#print len(inequality)
temp_ineq = sc.parallelize(inequality).repartition(1)
#print temp_ineq.collect()
in_eq_rdd = temp_ineq.zipWithIndex().map(lambda (a,b): (b,a))
#print "in_eq_rdd after zip: ", in_eq_rdd.collect()
#num_of_segments = x.count() / 5 #averaging at around 5 points per segment
#--------------------------------------- Find out "break" points in pattern--------------------------------------------------------------------

#assign indexes to y values, increment by 1 and 2 so that elements can be joined to find out diff later.
x0 = x.zipWithIndex().map(lambda (a,b): (b,a))
y0 = y.zipWithIndex().map(lambda (a,b): (b,a))
y1 = y0.map(lambda (a,b): (a+1,b) )
y2 = y0.map(lambda (a,b): (a+2,b) )
# join structure is like: [index, ((y0, y1) ,y2 )]. Hence the structure used in next map
#y[1] -> y[i+2], y[0][1] -> y[i+1], y[0][0] -> y[i]. Calculating diff (y[i+2]-y[i+1]) - (y[i+1]-y[i]) to find out breakpoint
#finding out difference in y coordinates of every consecutive pair. sort by desc order of difference
y_join = y0.join(y1).join(y2).map(lambda (x,y): (x, abs( (y[1]-y[0][1]) - (y[0][1] - y[0][0]) )) ).sortBy(lambda (a,b): b, False )

#caution: segments are derived from zip index. hence contain index of element NOT x element (i.e. year)
#picking up top difference elements - they form our segments
segments = sorted(y_join.map(lambda (a,b):a).take(num_of_segments))	#taking 2 additional segments. Because some times diff returns 2 consecutive points, which has to be discarded
segments = [0] + segments
segments.append(x.count())
#print segments
i=0
#remove consecutive points if included
#check for divide by 0. some prob in segmentation. print x1, check segment should have more than one element.
while i<len(segments)-1:
        if segments[i]+1==segments[i+1]:
                segments.remove(segments[i])
	else:
	        i=i+1	#increment i only in else, cause if element is deleted, then next element is now at same old i th position
print segments

#------------------------------------------------Calculate linear regression for every segment---------------------------------------------
seg=0
y_est = []
Ht_est= []
y = y.collect()

#for carry out linear regression for individual segment and also calculate Ht (Poverty Headcount)
while seg < len(segments)-1:
	start = segments[seg]
	end = segments[seg+1]
        #B1 = sum( (y_avg - yi)(x_avg - xi) ) / sum ( (x_avg - xi)(x_avg - xi) )
        #B0 = y_avg - B1*x_avg
	x1 = x0.filter(lambda (a,b): a >= start and a < end).map(lambda (a,b):float(b))	#filter x and y for current segment start and end
	y1 = y0.filter(lambda (a,b): a >= start and a < end).map(lambda (a,b):float(b))
	in_eq_rdd1 = in_eq_rdd.filter(lambda (a,b): a >= start and a < end).map(lambda (a,b):float(b))
	seg = seg + 1
	#print "--------------------------------------------------------------------------------------------------------------"
	#print "x1: ", x1.collect()
	#print "y1: ", y1.collect()
	#print "in_eq_rdd1: ", in_eq_rdd1.collect()
        x_avg = float(x1.sum())/x1.count()	#find out average
	y_count = y1.count()
        y_avg = float(y1.sum())/y_count

        i=0
        num=0
        den=0

	num = x1.zip(y1).map(lambda (a,b): (y_avg - b) * (x_avg - a)).sum()	#numerator and denominator as per above formula of linear regression
	den = den + x1.map(lambda a: (x_avg - a) * (x_avg - a) ).sum()

        B1 = num / den		#B0 and B1 (Beta 0 and Beta 1) calculation
        B0 = y_avg - B1*x_avg

	y_est = y_est + x1.map(lambda a: B0 + B1*a).collect()	#calculate estimated linear regression using Beta0 and Beta 1 (finally!!!)

	#-----------------------------------------------calculate Ht - Poverty headcount
	yt = y1.repartition(1).zip(in_eq_rdd1.repartition(1))
	#print "yt: ", yt.collect()
	Ht = yt.map(lambda(a,b): -1*(log( pow(e,log(a) + b*b/2 )/z))/b + b*b   ) 
	#print "Ht expr: ", Ht.collect()
	#now calculate phi = 0.5 * ( 1 + sign(x) * sqrt( ( 1 - e to power ( -2x*x/pi ))  )  )
	Ht = Ht.map(lambda a: 100 * 0.5 *  ( 1 +  sign(a)* sqrt(  1 - pow( e, -2*a*a/pi  )     )  )  )	#multiply by 100 to get percentage
	Ht_est = Ht_est + Ht.collect()
	#print "Ht: ", Ht_est
#---------------------------------------extend x with upcoming years and predict using last segment's B0 and B1
#print "--------------------------------------------------------------------------------------------------------------------------"
x=x.collect()
predict_start = x[len(x)-1] + 1		#first year of prediction = last year of available data + 1

x_est = x + range(predict_start, predict_end+1)	#adding + 1 to accomodate end year as well
y_new = [] #new array to store predicted yt values. to be used later to predict Ht

for i in range(predict_start, predict_end+1):
	y_est.append(B0 + B1*i)
	y_new.append(B0 + B1*i)

#-----------------------------------------------calculate Ht - Poverty headcount for estimated yt----------------------------------
y_count = len(y_new)
y1 = sc.parallelize(y_new)

#interpolating std deviation for future
inequality = np.interp(range(predict_start, predict_end+1),x,inequality)  #using seperate rdd to keep array for later use
in_eq_rdd = sc.parallelize(inequality)
yt = y1.zip(in_eq_rdd)

Ht = yt.map(lambda(a,b): -1*(log( pow(e,log(a) + b*b/2 )/z))/b + b*b   )
#now calculate phi = 0.5 * ( 1 + sign(x) * sqrt( ( 1 - e to power ( -2x*x/pi ))  )  )
Ht = Ht.map(lambda a: 100 * 0.5 *  ( 1 +  sign(a)* sqrt(  1 - pow( e, -2*a*a/pi  )     )  )  ) #multiply by 100 to get percentage
Ht_est = Ht_est + Ht.collect()
#print "Ht: ", Ht_est

#plt.plot(x,y,'bs', x_est, y_est, 'g--', x_est, Ht_est, 'r--')
#plt.plot( x_est, Ht_est,'r--',PPP_YEAR, PPP, 'b--')
#plt.show()

plt.xlabel('year')
plt.ylabel('Poverty headcount ratio at $2 a day (PPP) (% of population)')

line1, = plt.plot(x_est,Ht_est,linestyle="solid", marker='o', color="blue")
line2, = plt.plot(PPP_YEAR,PPP, linestyle="solid", marker="o", color="red")

plt.legend([line1, line2], ["Estimate / Prediction","World Bank estimate"])
plt.title("Poverty headcount ratio at $2 a day (PPP) (% of population) for " + country)
plt.show()


