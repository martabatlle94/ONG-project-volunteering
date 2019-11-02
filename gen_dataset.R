library(data.table)
library(lubridate)
library(stringr)

ReplaceNA_dt = function(DT, replace = 0) {
  for (i in names(DT))
    DT[is.na(get(i)), (i) := replace]
  
  DT
}

# params ----
fecha_min   <- 200001
fecha_datos <- 201808

path.in     <- "Dataton/datathon_train/"
path.out    <- "Dataton/datos_tratados/"

fecha_datos_3m <- as.numeric(format(as.POSIXlt.character(paste0(fecha_datos,"01"), format = "%Y%m%d") - months(3), "%Y%m" ))
fecha_datos_6m <- as.numeric(format(as.POSIXlt.character(paste0(fecha_datos,"01"), format = "%Y%m%d") - months(6), "%Y%m" ))

tgt_m1 <- as.numeric(format(as.POSIXlt.character(paste0(fecha_datos,"01"), format = "%Y%m%d") + months(4), "%Y%m" ))
tgt_m2 <- as.numeric(format(as.POSIXlt.character(paste0(fecha_datos,"01"), format = "%Y%m%d") + months(5), "%Y%m" ))
tgt_m3 <- as.numeric(format(as.POSIXlt.character(paste0(fecha_datos,"01"), format = "%Y%m%d") + months(6), "%Y%m" ))

 
# # Datos By Id_Cliente ----
tb.dt <- fread(paste0(path.out, "tablin_final.csv"), dec = ",")
#setnames(datos_miembro.dt, "ID", "IDMIEMBRO")

drop_vars        <- names(tb.dt)[names(tb.dt) %like% "alta" ]
eval(parse(text = paste0("tb.dt[, c('baj_fech','", paste0(drop_vars, collapse = "', '"),"') := NULL]")))

datos_miembro.dt <- unique(datos_miembro.dt)
fwrite(datos_miembro.dt, paste0(path.out,"/datos_miembro.csv"))
rm(tb.dt)


#Datos MIEMBRO & FECHA ----


altas_2.dt <- fread(paste0(path.in, "2.ALTASYBAJAS_train.txt"))  # Contiene medios, merge con medios

altas_2.dt <- altas_2.dt[IDREGISTRO == 0, ]
altas_2.dt[ , FECHA := as.numeric(format(as.POSIXct(FECHA, format = "%d/%m/%Y"), "%Y%m%d")) ]
altas_2.dt <- unique(altas_2.dt[FECHA >= as.numeric(paste0(fecha_min,"01")), ])
altas_2.dt <- merge(altas_2.dt, altas_2.dt[, .N, by = c("IDMIEMBRO", "FECHA")][N == 1, ] ) # people with 2 altas in the same day
altas_2.dt[, N := NULL]

alta_last.dt <- altas_2.dt[ FECHA <=  as.numeric(paste0(fecha_datos,31)), .(max(FECHA), .N), by = "IDMIEMBRO"]
setnames(alta_last.dt, c("V1"), c("last"))

alta_last.dt[ , antiguedad := 
  round(
    as.numeric(
      difftime( 
        as.POSIXct(paste0(fecha_datos,01), format = "%Y%m%d"),
        as.POSIXct(paste0(last), format = "%Y%m%d")
        , units = "days")
    ), 
  0)
  ]

# Una vez tenemos la fecha de la ultima alta, juntamos por miembro y fecha
# NO QUIERO AQUELLOS QUE NO TIENEN UNA FECHA DE ALTA  -> left join con alta_last.dt
altas_fin.dt <-  merge(alta_last.dt, altas_2.dt, by.x = c("IDMIEMBRO","last"), by.y = c("IDMIEMBRO", "FECHA"), all.x = T)
altas_fin.dt[, IDMEDIO := tolower(IDMEDIO)]

medios.dt    <- fread(paste0(path.in, "/Medios2.txt"))[, .(MEDIO, grupo2, grupomedio2)]
medios.dt[, MEDIO := tolower(MEDIO)]

altas_fin.dt <- merge(altas_fin.dt, medios.dt, by.x = "IDMEDIO", by.y = "MEDIO", all.x = T)
altas_fin.dt[, IDMEDIO   := NULL]
altas_fin.dt[, IDFAMILIA := NULL]


altas_fin.dt[, last := as.numeric(format(as.POSIXct(as.character(last), format = "%Y%m%d"), "%Y%m")) ]

fwrite(altas_fin.dt, paste0(path.out,"/altas_",fecha_datos,".csv"))
rm(altas_fin.dt, medios.dt, alta_last.dt, altas_2.dt)





# Interaciones

interacciones_4.dt        <- fread(paste0(path.out,"/interacciones_4_fin.csv"))


interacciones_4_1m.dt     <- interacciones_4.dt[FECHA >= fecha_min & FECHA <= fecha_datos, lapply(.SD, sum), .SDcols = names(interacciones_4.dt)[3:ncol(interacciones_4.dt)], by = c("IDMIEMBRO")]
#interacciones_4_3m.dt     <- interacciones_4.dt[FECHA >= fecha_datos_3m & FECHA <= fecha_datos, lapply(.SD, sum), .SDcols = names(interacciones_4.dt)[3:ncol(interacciones_4.dt)], by = c("IDMIEMBRO")]
interacciones_4_6m.dt     <- interacciones_4.dt[FECHA >= fecha_datos_6m & FECHA <= fecha_datos, lapply(.SD, sum), .SDcols = names(interacciones_4.dt)[3:ncol(interacciones_4.dt)], by = c("IDMIEMBRO")]

#setnames(interacciones_4_3m.dt, names(interacciones_4_3m.dt), paste0(names(interacciones_4_3m.dt),"_3m"))
setnames(interacciones_4_6m.dt, names(interacciones_4_6m.dt), paste0(names(interacciones_4_6m.dt),"_6m"))


interacciones_4.dt <- merge(interacciones_4_1m.dt, interacciones_4_6m.dt, by.x = "IDMIEMBRO", by.y = "IDMIEMBRO_6m", all.x = T)
rm(interacciones_4_1m.dt, interacciones_4_6m.dt)


ReplaceNA_dt(interacciones_4.dt)

#Guardar Intreacciones
fwrite(interacciones_4.dt, paste0(path.out,"/interacciones_4_",fecha_datos,".csv"))
rm(interacciones_4.dt)

# Donativos por persona -- 

donativos.dt <- fread(paste0(path.in,"/10.APORTACIONES_train.txt"), dec = ",")
donativos.dt <- donativos.dt[TIPOAPORTACION == "D",]
donativos.dt[, FECHACOBRO := (as.numeric(format(as.POSIXct(FECHACOBRO, format = "%d/%m/%y"),"%Y%m"))) ]

donativos_all.dt <- donativos.dt[ FECHACOBRO >= fecha_min      & FECHACOBRO <= fecha_datos, .(.N, sum(IMPORTE), mean(IMPORTE)), by = c("IDMIEMBRO") ]
donativos_6m.dt  <- donativos.dt[ FECHACOBRO >= fecha_datos_6m & FECHACOBRO <= fecha_datos, .(.N, sum(IMPORTE), mean(IMPORTE)), by = c("IDMIEMBRO") ]

gr_names_don <- c("donacion_N","donacion_sum", "donacion_avg")
setnames(donativos_all.dt,c("N", paste0("V",2:3)), gr_names_don)
setnames(donativos_6m.dt, c("N", paste0("V",2:3)), paste0(gr_names_don,"_6m"))

donativos.dt  <- merge(donativos_all.dt, donativos_6m.dt, by = "IDMIEMBRO", all = T)
ReplaceNA_dt(donativos.dt)

fwrite(donativos.dt, paste0(path.out,"donativos_",fecha_datos,".csv"))
rm(donativos.dt, donativos_all.dt, donativos_6m.dt)

# Aportaciones
# d10.dt <- paste0(path.in,"10.APORTACIONES_train.txt", dec = ",")
# 
# d10.dt <- d10.dt[ 
#            TIPOAPORTACION == "S"    &
#             ESTADO %in% c("C")
#          ,  .(FECHACOBRO, IDMIEMBRO, ESTADO, IDCAMPANYA, IDESTADOMIEMBRO, IDFORMAPAGO, IDMEDIO, IDPERIODICIDADFP, IMPORTE, NUMERODEVOLUCIONES)
#          ]
# 
# d10.dt[ , FECHACOBRO := format(as.POSIXct(FECHACOBRO, format = "%d/%m/%y"), "%Y%m")  ]
# fwrite(d10.dt, paste0(path.out,"10.APORTACIONES_cobradas_socio_Fecha.csv")
d10.dt <- fread(paste0(path.out,"10.APORTACIONES_cobradas_socio_Fecha.csv"))


ap.dt    <- d10.dt[FECHACOBRO >= fecha_min      & FECHACOBRO <= fecha_datos, .(mean(IMPORTE), sum(NUMERODEVOLUCIONES), sum(IMPORTE), mean(IDPERIODICIDADFP), min(IDPERIODICIDADFP) ), by = IDMIEMBRO]
ap_6m.dt <- d10.dt[FECHACOBRO >= fecha_datos_6m & FECHACOBRO <= fecha_datos, .(mean(IMPORTE), sum(NUMERODEVOLUCIONES), sum(IMPORTE), mean(IDPERIODICIDADFP), min(IDPERIODICIDADFP) ), by = IDMIEMBRO]


gr_names_ap <- c("IMPORTE_avg", "NUMERODEVOLUCIONES_sum", "IMPORTE_sum", "IDPERIODICIDAD_avg", "IDPERIODICIDADFP_min")

setnames(ap.dt, paste0("V",1:5), gr_names_ap)
setnames(ap_6m.dt, paste0("V",1:5), paste0(gr_names_ap,"_6m"))

ap.dt <- merge(ap.dt, ap_6m.dt, by = "IDMIEMBRO", all.x = T)
ReplaceNA_dt(ap.dt)

fwrite(ap.dt, paste0(path.out,"/10.APORTACIONES_", fecha_datos,".txt"))
rm(ap_6m.dt, ap.dt, d10.dt)

# Target

bajas.dt <- fread(paste0(path.out,"5_bajas_final.csv"))


bajas.dt[, baj_fech := as.numeric(format(as.POSIXct(baj_fech, format = "%d/%m/%Y"), "%Y%m")) ]


# Target
fwrite(bajas.dt[baj_fech %in% c(tgt_m1,tgt_m2,tgt_m3), 
                .(IDMIEMBRO, baja_motivo_clst, baj_fech)]
       ,paste0(path.out,"/target_for_", fecha_datos, ".csv"))

# Bajas hasta, para filtrar
fwrite( bajas.dt[ baj_fech >= fecha_min & baj_fech <= fecha_datos, .N, by = "IDMIEMBRO" ]
        , paste0(path.out,"/bajas_hasta_", fecha_datos, ".csv"))



rm(bajas.dt)

## Get it All together ----

#Miembro
datos_miembro.dt   <- fread(paste0(path.out,"/datos_miembro.csv"))

#Miembro + fecha -- Fecha fijada por fecha_datos  - > key = miembro para todos
altas_fin.dt       <- fread(paste0(path.out,"altas_",fecha_datos,".csv"))
interacciones_4.dt <- fread(paste0(path.out,"interacciones_4_",fecha_datos,".csv"))
donativos.dt       <- fread(paste0(path.out,"donativos_",fecha_datos,".csv"))
bajas.dt           <- fread(paste0(path.out,"bajas_hasta_", fecha_datos, ".csv")) # filtro

ap.dt              <- fread(paste0(path.out,"10.APORTACIONES_", fecha_datos,".txt"))


setnames(altas_fin.dt, setdiff(names(altas_fin.dt), "IDMIEMBRO"), paste0(setdiff(names(altas_fin.dt), "IDMIEMBRO"),"_alta"))
dst.dt <- merge(altas_fin.dt, interacciones_4.dt, by = "IDMIEMBRO", all.x = T)
rm(altas_fin.dt, interacciones_4.dt)

dst.dt <- merge(dst.dt, donativos.dt, by = "IDMIEMBRO", all.x = T)
rm(donativos.dt)

setnames(bajas.dt, "N", "N_baj")
dst.dt <- merge(dst.dt, bajas.dt, by = "IDMIEMBRO", all.x = T)
dst.dt <- dst.dt[is.na(N_baj), ] # filtrampos por los que no tienen ninguna baja previa
rm(bajas.dt)

dst.dt <- merge(dst.dt, ap.dt, by = "IDMIEMBRO", all.x = T)
rm(ap.dt)
ReplaceNA_dt(dst.dt)

dst.dt <- merge(dst.dt, datos_miembro.dt, by = "IDMIEMBRO", all.x = T)

# add the target
bajas_tgt.dt       <- fread(paste0(path.out,"/target_for_201808.csv"))

dst.dt             <- merge(dst.dt, bajas_tgt.dt[, .(IDMIEMBRO, 1)], by = "IDMIEMBRO", all.x = T)
setnames(dst.dt, "V2", "target")
dst.dt[is.na(target), target := 0]

fwrite(dst.dt, paste0(path.out,"dataset_msf_", fecha_datos,".csv"))
