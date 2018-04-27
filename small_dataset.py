def hadoop(filename):
    infile=open(filename,"r")
    file=infile.read()
    count = 0
    file= list(file)

    for i in range(len(file)-1):
        if (file[i] == "\"" and (file[i+1]).isnumeric() and file[i+2] == "\"" and file[i+3] == "\n"):
            count = count +1
            if count == 100:
                break
    
    filewrite = open("output.txt","w") 
    filewrite.write("".join(file))
            
hadoop("yelp_review.csv")
