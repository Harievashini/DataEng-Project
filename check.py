import json
trip=[]
final=[]
for i in range(15,23):
    filename="2021-01-"+str(i)+".json"
    print(filename)
    f=open(filename,"r")
    data=json.load(f)
    for i in data:
        for k,v in i.items():
            #print(k,v)
            if k=="GPS_LATITUDE":
                if v!="":
                    if float(v) not in trip:
                        trip.append(float(v))
                        #print(trip)
                break
    trip.sort()
    #print(trip)
    final.append(trip[0])
    final.append(trip[-1])
    #print(final)
    trip=[]
#print("\n")
final.sort()
print(final)
print("min",final[0],"max",final[-1])
