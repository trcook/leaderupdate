require(data.table)
require(countrycode)
dat<-data.table(read.csv('./Elections and Leaders - Leaders.csv'))
dat[,cowc:=countrycode(ccode,"cown","cowc")]
names(dat)
output = dat[is.na(eyear),.(actor1country = cowc,actor1name = leader)]
write.csv(output,file='./current_office.csv')
