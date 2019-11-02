library(data.table)
library(h2o)
dst.dt <- fread("C:/Users/POR633639/Documents/Dataton/datos_tratados/dataset_msf_201808.csv", dec = ",")


dst.dt[, .N, by = "target"]
h2o.init()

data.h2o       <- h2o.importFile("C:/Users/POR633639/Documents/Dataton/datos_tratados/msf.csv")

data_split.h2o <- h2o.splitFrame(data.h2o, ratios = 0.6, seed = 299)
rm(data.h2o)

train.h2o      <- data_split.h2o[[1]]
test.h2o       <- data_split.h2o[[2]]
rm(data_split.h2o)


dst_vars <- setdiff(names(train.h2o), c("target"))


model.h2o <- h2o.gbm(
    x                = dst_vars
  , y                = "target" 
  , training_frame   = train.h2o
  , validation_frame = test.h2o
  , balance_classes  = T
            )

h2o.auc(model.h2o)

model.h2o

h2o.ggplot.roc( perf.train = h2o.performance(model.h2o, train = T)
                ,perf.test = h2o.performance(model.h2o, train = F))


h2o.varimp(model.h2o)
h2o.varimp_plot(model.h2o)


h2o.getTypes(train.h2o[, "TELEFONOFIJO"])
