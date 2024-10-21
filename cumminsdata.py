import pandas as pd
import openpyxl
import odf
from datetime import datetime, timedelta
from collections import deque

Actualizar='NO'
dbm_reporte_base_ruta='Reporte-Base-2024-10-11.xlsx'
mant_mins_ruta='MANT MINS 930E-980E-830E, 03-10-2024.xlsm'
camiones_500_rango=['CA184','CA185','CA186','CA187','CA188','CA189','CA190']
fechasxcargar='2023-02-27','2024-10-03'

reportebase_ruta='metadataC.xlsx'
mpyaceitetab_ruta='mpyaceibase.xlsx'
asarco_ruta='asarco.xlsx'
avanhrs_ruta='avanhrs+1.xlsx'
avanhrs_hrs_mes_rango='2024-07-31','2024-10-03'

print('Cargando Datos')

dfrbase=pd.read_excel(reportebase_ruta)
mpyaceite=pd.read_excel(mpyaceitetab_ruta)
asarco=pd.read_excel(asarco_ruta)
avanhrs=pd.read_excel(avanhrs_ruta)
dbm_reporte_base=pd.read_excel(dbm_reporte_base_ruta)

print('Datos Cargados')

excel_sheets = pd.read_excel(mant_mins_ruta, sheet_name=None, engine='openpyxl')

meses=['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']

indexsheets=[]
for a in meses:
    for b in excel_sheets: 
     if a in b:
        indexsheets.append(b)

for b in indexsheets:
    excel_sheets[b]=excel_sheets[b].drop(index=0)
    excel_sheets[b].columns=excel_sheets[b].iloc[0]
    excel_sheets[b]=excel_sheets[b].iloc[:, :-2] 
    excel_sheets[b]=excel_sheets[b].drop(index=1)
    excel_sheets[b]=excel_sheets[b].reset_index(drop=True)

mergepivot=excel_sheets[indexsheets[0]].columns[0]


merged_df = excel_sheets[indexsheets[0]]

# Iterar sobre los DataFrames restantes y hacer el merge
for df in indexsheets[1:]:
    merged_df = pd.merge(merged_df, excel_sheets[df], on=mergepivot, how='outer')
merged_df=merged_df[~merged_df['EQUIPOS 930E'].isin(['EQUIPOS 830E','EQUIPOS 980E'])]
merged_df=merged_df.reset_index(drop=True)
merged_df=merged_df.dropna(axis=1, how='all')
merged_df=merged_df.dropna(axis=0, how='all')



def generar_lista_fechas1(fecha_inicio_dt, fecha_fin_dt):
 
   
   

    lista_fechas = []
    fecha_actual = fecha_inicio_dt

    while fecha_actual <= fecha_fin_dt:
     
        lista_fechas.append(fecha_actual.strftime("%Y-%m-%d 00:00:00"))
      
        fecha_actual += timedelta(days=1)

    return lista_fechas

date_list1=generar_lista_fechas1(asarco[asarco.columns[len(asarco.columns)-1]].name,merged_df[merged_df.columns[len(merged_df.columns)-1]].name)
date_list1=date_list1[1:]


for c in range(len(date_list1)):
    date_list1[c]=datetime.strptime(date_list1[c], "%Y-%m-%d %H:%M:%S")
date_list1.append('EQUIPOS 930E')


time_to_add=merged_df[date_list1]


asarco=pd.merge(asarco,time_to_add,left_on='Equipo',right_on='EQUIPOS 930E',how='left').iloc[:, :-1]

datelist2=generar_lista_fechas1(dfrbase['Fecha Inicio'].max(),pd.Timestamp(fechasxcargar[1]))
for c in range(len(datelist2)):
    datelist2[c]=pd.Timestamp(datelist2[c])

dbm_report_toadd=dbm_reporte_base[dbm_reporte_base['Fecha Inicio'].isin(datelist2)]

dfrbase=pd.concat([dfrbase, dbm_report_toadd], axis=0, ignore_index=True)

def generar_lista_fechas(fecha_inicio, fecha_fin):
 
   
    fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d")

    lista_fechas = []
    fecha_actual = fecha_inicio_dt

    while fecha_actual <= fecha_fin_dt:
     
        lista_fechas.append(fecha_actual.strftime("%Y-%m-%d 00:00:00"))
      
        fecha_actual += timedelta(days=1)

    return lista_fechas

ufah=avanhrs[avanhrs.columns[len(avanhrs.columns)-1]].name-timedelta(days=1)

date_list=generar_lista_fechas(ufah.strftime('%Y-%m-%d'),fechasxcargar[1])
for f in date_list[2:]:
    avanhrs[datetime.strptime(f, "%Y-%m-%d %H:%M:%S")]=None

print('Llenando datos de horas avanzadas')
asarcolite=asarco[['Equipo']]
avanhrslite=avanhrs[['Unidad']]
for index, g in enumerate(avanhrs['Unidad']):
    for h in range(2,len(date_list)):
     try:
       as1val=asarco.at[asarcolite[asarcolite['Equipo']=='CA'+str(g)].index[0],datetime.strptime(date_list[h-1], "%Y-%m-%d %H:%M:%S")]
       as2val=asarco.at[asarcolite[asarcolite['Equipo']=='CA'+str(g)].index[0],datetime.strptime(date_list[h-2], "%Y-%m-%d %H:%M:%S")]
       asvalu=as1val-as2val
      #  avhrsant=avanhrs[avanhrs['Unidad']==g][datetime.strptime(date_list[h-1], "%Y-%m-%d %H:%M:%S")].values[0]
       avhrsant=avanhrs.at[avanhrslite[avanhrslite['Unidad']==g].index[0],datetime.strptime(date_list[h-1], "%Y-%m-%d %H:%M:%S")]
       if isinstance(avhrsant, (int, float)) and isinstance(asvalu, (int, float)):
        valor=avhrsant+asvalu
        #valor=round(valor)
        avanhrs[datetime.strptime(date_list[h], "%Y-%m-%d %H:%M:%S")][index]=valor
     except Exception as e:
            pass
print('Se completaron las horas avanzadas')

if Actualizar=='SI':
 print('Actualizando datos base')
 asarco.to_excel(asarco_ruta,index=False)
 dfrbase.to_excel(reportebase_ruta,index=False)
 avanhrs.to_excel(avanhrs_ruta,index=False)
 print('Se actualizaron los datos base')


avanhrs['VALOR MAX']=None
for index,i in enumerate(avanhrs['Unidad']):
    avanhrs['VALOR MAX'][index]=avanhrs.drop(['Flota', 'Unidad','Estado','ESN', 'PS','Arreglo motor','Tipo contrato'],axis=1).iloc[index].max()

avanhrs['Equipo']=avanhrs['Unidad']
avanhrs['Horas']=avanhrs['VALOR MAX']

avanhrs['HRSMES']=None


for index,j in enumerate(avanhrs['Unidad']):
  try:
    valor1=avanhrs[datetime.strptime(avanhrs_hrs_mes_rango[0], "%Y-%m-%d")][index]
  except Exception as e:
     valor1=0
  try:
    valor2=avanhrs[datetime.strptime(avanhrs_hrs_mes_rango[1], "%Y-%m-%d")][index]
  except Exception as e:
     valor2=0
  if isinstance(valor2, (int, float)) and isinstance(valor1, (int, float)):
    valorfinal = valor2 - valor1
  else:
    valorfinal = 0 
  avanhrs['HRSMES'][index]=valorfinal

dfrbase['caex']=None
dfrbase['hrs detencion']=None
dfrbase['hrs de hoy']=None
dfrbase['DIFERN']=None
dfrbase['MES']=None
dfrbase['SEMANA']=None
dfrbase['AÑO']=None

dfrbase['caex']=dfrbase['Unidad']


indexavan=avanhrs[['Unidad']]
for index, k in enumerate(dfrbase['caex']):
    unidadb=k
    fechab=dfrbase['Fecha Inicio'][index]
    ibusqueda=indexavan[indexavan['Unidad']==unidadb].index
    try:
     valorf1=avanhrs.at[ibusqueda[0],fechab.to_pydatetime()]
     dfrbase['hrs detencion'][index]=valorf1
    except Exception as e:
       pass


dataliteavanhrs=avanhrs[['Equipo','Horas']]
dfrbase['hrs de hoy']=pd.merge(dfrbase,dataliteavanhrs,left_on='caex',right_on='Equipo',how='left')['Horas']

def difernfunc(hrsdec, hrshoy):
  
    if hrshoy is not None and hrsdec is not None and isinstance(hrshoy, (int, float)) and isinstance(hrsdec, (int, float)):
        if hrshoy > hrsdec:
            return hrshoy - hrsdec
        else:
            return 0
    else:
      
        return None
    

dfrbase['DIFERN']=dfrbase.apply(lambda row: difernfunc(row['hrs detencion'], row['hrs de hoy']), axis=1)

dfrbase['MES']=dfrbase['Fecha Inicio'].apply(lambda x:x.month)
dfrbase['SEMANA']=dfrbase['Fecha Inicio'].apply(lambda x:x.isocalendar().week)
dfrbase['AÑO']=dfrbase['Fecha Inicio'].apply(lambda x:x.year)

mp=dfrbase[dfrbase['Tipo']=='MP']
mp=mp[mp['Categoría']=='Inicial']
mp=mp[['Unidad','Fecha Inicio','Sintoma','DIFERN']]
mp=mp.sort_values(by=['Unidad','Fecha Inicio'], ascending=[True, True])
mp.reset_index(inplace=True)
mp=mp[['Unidad','Fecha Inicio','Sintoma','DIFERN']]


mp['HRS MP']=None
counter=0
for o in range(len(mp['Unidad'])):
    if counter==0:
        mp['HRS MP'][o]=0
        counter+=1
    else:
        if mp['Unidad'][o]==mp['Unidad'][o-1]:
            value1=mp['DIFERN'][o]
            value2=mp['DIFERN'][o-1]
            if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
             fvalue=value2-value1
             if pd.isnull(fvalue):
                fvalue=0
             mp['HRS MP'][o]=fvalue
            else:
              mp['HRS MP'][o]=0 
        else:
           mp['HRS MP'][o]=0
hrsmp=mp


hrsultmp=({
    'unidad':[None],
    'hrsult1':[None],
    'hrsult2':[None],
    'tipo':[None]
})
hrsultmp=pd.DataFrame(hrsultmp)
hrsultmp=hrsultmp.reindex(range(len(hrsmp['Unidad'].unique())))
hrsultmp['unidad']=hrsmp['Unidad'].unique()


for index, b in enumerate(hrsultmp['unidad']):
    newfilter=b
    datafilt=hrsmp[hrsmp['Unidad']==newfilter]
    datelist=datafilt['Fecha Inicio'].to_list()

    hrsultmp['hrsult2'][index]=max(datelist)

for index, b in enumerate(hrsultmp['unidad']):
    filtro=b
    datafilt=hrsmp[hrsmp['Unidad']==filtro]
    datelist=datafilt['Fecha Inicio'].to_list()
    valuef=datafilt[datafilt['Fecha Inicio']==max(datelist)]['Sintoma'].values[0]
    hrsultmp['tipo'][index]=valuef

for index, b in enumerate(hrsultmp['unidad']):
    filtro=b
    datafilt=hrsmp[hrsmp['Unidad']==filtro]
    datelist=datafilt['Fecha Inicio'].to_list()
    valuef=datafilt[datafilt['Fecha Inicio']==max(datelist)]['DIFERN'].values[0]
    if not isinstance(valuef,str):
       if valuef is None or pd.isna(valuef):
         valuef=0
       else:
         valuef=round(valuef)  
    hrsultmp['hrsult1'][index]=valuef


hrsacei=dfrbase[dfrbase['Elemento']=='Aceite Motor']
hrsacei=hrsacei[hrsacei['Solución']=='Cambio']
hrsacei=hrsacei[['Unidad','Fecha Inicio','Tipo','DIFERN']]
hrsacei=hrsacei.sort_values(by=['Unidad','Fecha Inicio'], ascending=[True, True])
hrsacei.reset_index(inplace=True)
hrsacei=hrsacei[['Unidad','Fecha Inicio','Tipo','DIFERN']]


hrsacei['HRS ACEI']=None
counter=0
for o in range(len(hrsacei['Unidad'])):
    if counter==0:
        hrsacei['HRS ACEI'][o]=0
        counter+=1
    else:
        if hrsacei['Unidad'][o]==hrsacei['Unidad'][o-1]:
            value1=hrsacei['DIFERN'][o]
            value2=hrsacei['DIFERN'][o-1]
            if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
              fvalue=value2-value1
              if pd.isnull(value1):
                 fvalue=0
              hrsacei['HRS ACEI'][o]=fvalue
            else:
               hrsacei['HRS ACEI'][o]=0
        else:
           hrsacei['HRS ACEI'][o]=0


subhracei=({
    'CAEX':[None],
    'hrsaceite':[None],
    'fecha':[None],
    'tipo':[None],
})
subhracei=pd.DataFrame(subhracei)
subhracei=subhracei.reindex(range(hrsacei['Unidad'].nunique()))
subhracei['CAEX']=hrsacei['Unidad'].unique()


for indice,c in enumerate(subhracei['CAEX']):
    filtro=c
    datafilt=hrsacei[hrsacei['Unidad']==filtro]
    datelist=datafilt['Fecha Inicio'].to_list()
    valuef=datafilt[datafilt['Fecha Inicio']==max(datelist)]['DIFERN'].values[0]
    if not isinstance(valuef, str) and pd.notnull(valuef):
        valuef=round(valuef)
    subhracei['hrsaceite'][indice]=valuef


for indice,c in enumerate(subhracei['CAEX']):
    filtro=c
    datafilt=hrsacei[hrsacei['Unidad']==filtro]
    datelist=datafilt['Fecha Inicio'].to_list()


    subhracei['fecha'][indice]=max(datelist)

for indice,c in enumerate(subhracei['CAEX']):
    filtro=c
    datafilt=hrsacei[hrsacei['Unidad']==filtro]
    datelist=datafilt['Fecha Inicio'].to_list()
    valuef=datafilt[datafilt['Fecha Inicio']==max(datelist)]['Tipo'].values[0]
    subhracei['tipo'][indice]=valuef

lista_camiones=[]
for a in camiones_500_rango:
    lista_camiones.append(int(a.replace('CA','')))

mpyaceite['Fecha Ultima Mp']=None
mpyaceite['HRS MP']=None
def desviacion2t(x):
    t=x.replace('%','')
    t=int(t)
    if t>0:
        return x
    else:
        return '%0'
for indice,d in enumerate(mpyaceite['CAEX2']):
    filtro=d
    datafilt=hrsultmp[hrsultmp['unidad']==filtro]
    mpyaceite['Fecha Ultima Mp'][indice]=datafilt['hrsult2'].values
    mpyaceite['HRS MP'][indice]=datafilt['hrsult1'].values
mpyaceite['CAEX3']=mpyaceite['CAEX2']
mpyaceite['HRS ACEITE']=None
mpyaceite['TIPO']=None
mpyaceite['FECHA ULTIMO CAMBIO']=None
for index, e in enumerate(mpyaceite['CAEX3']):
    filtro1=e
    datafilt1=subhracei[subhracei['CAEX']==filtro1]
    mpyaceite['HRS ACEITE'][index]=datafilt1['hrsaceite'].values
    mpyaceite['TIPO'][index]=datafilt1['tipo'].values
    mpyaceite['FECHA ULTIMO CAMBIO'][index]=datafilt1['fecha'].values
mpyaceite['PROXIMA MP']=None
mpyaceite['DESVIACION ACEITE']=None
mpyaceite['PROXIMA MP'] = mpyaceite['HRS ACEITE'].apply(lambda x: str(round(1000 - int(x))) if pd.notnull(x) and not pd.isna(x) else '0')
for index,b in enumerate(mpyaceite['CAEX2']):
    if b in lista_camiones:
       if pd.notnull(mpyaceite['HRS ACEITE'][index]) and not pd.isna(mpyaceite['HRS ACEITE'][index]): 
        proximamp=str(round(500-mpyaceite['HRS ACEITE'][index]))
       else:
          proximamp=0
       mpyaceite['PROXIMA MP'][index]=proximamp
mpyaceite['DESVIACION ACEITE']=mpyaceite['HRS ACEITE'].apply(lambda x:'%'+str(round((((int(x)*1)/1000)-1)*100))if pd.notnull(x) and not pd.isna(x) else '0')
mpyaceite['DESVIACION ACEITE2']=mpyaceite['DESVIACION ACEITE'].apply(desviacion2t)


mpyaceite.to_excel('MPyAceite.xlsx',index=False)
print('El Programa a Finalizado')
    