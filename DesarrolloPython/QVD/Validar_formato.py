#!/usr/bin/env python
# coding: utf-8

# ----------Librerias utilizadas----------    

# In[1]:


import re
import pandas as pd
from qvd import qvd_reader


# ----------lectura y filtro del excel. Pasa al dfDatos----------

# In[2]:


ruta = "./Parametros.xlsx"
# Variable de la ruta relativa.
dfxls = pd.read_excel(ruta, sheet_name="Validaciones")
# Asignación de la hoja de excel Validaciones a un df
dfDatos = dfxls[dfxls["Tipo Validacion"].isin(["Formato"])]
# Se filtra por el tipo de validación Formato
dfDatos.head()


# ----------Creacion de rutas que se usan en el codigo----------

# In[3]:


dfRuta = pd.read_excel(ruta, sheet_name="Ejecucion")
#Lee la hoja Ejexcución del EXCEL
dfRutaCSV = dfRuta[dfRuta["Parametro"].isin(["PathPython"])]
#Busca en parametros el que contiene el PathPython
dfRutaCSV1 = dfRuta[dfRuta["Parametro"].isin(["PathDestin"])]
dfRutaCSV = dfRutaCSV.reset_index()
#Resetea los indices del dfRustas para facilitar la busqueda


# -------- Ciclo for recorre el df datos en busca de las tablas a validar y lectura de qvd---------

# In[4]:


#Ciclo lee el dfDatos en busca de las tablas a validar
#El nombre del qvd se guarda en la variable qvd
for i in range(len(dfDatos)):
    tabla = (dfDatos.iloc[i]["Esquema"]+"_"+dfDatos.iloc[i]["Tabla"])
    pathCSV1 = (dfRutaCSV1.iloc[0]["Valor"]) 
    qvd = pathCSV1+ "\\"  +tabla +".qvd"    
    #print(tabla)
    #print(qvd)
    dfqvd = qvd_reader.read (qvd)


# ----------Creacion de variables vacias que se llenaran con las validaciones de expresiones regulares----------

# In[5]:


#Variable conteo para guardar el resultado del Groupby
dfqvd["ValoresValidacion"] = ""
#Creacion de variable esquema que va a contener tabla.campo
dfqvd["Campo"] = ""


# ----------Validación de expresión regular----------

# In[6]:


# Ciclo recorre el dfDatos y asigna a las variables campo, Formato y tabla el contenido de dfDatos para la validación
listas1 =[]

for i in range(len(dfDatos)):
    campo = (dfDatos.iloc[i]["Campo"])
    Formato = (dfDatos.iloc[i]["Valores"])
    tabla = (dfDatos.iloc[i]["Esquema"]+"_"+dfDatos.iloc[i]["Tabla"])
    dfqvd[tabla+"."+campo] = dfqvd[campo]
    dfqvd = dfqvd.replace({campo: Formato},{campo:"Valido"}, regex=True)
    tabla_campo = (dfDatos.iloc[i]["Esquema"]+"_"+dfDatos.iloc[i]["Tabla"]+"."+dfDatos.iloc[i]["Campo"])    
    #Lleamos los valores nulos con "No valido"
    dfqvd1 = dfqvd.fillna("Nulo")
    listas1.append([tabla_campo,campo])
    #print(campo)
    #print(Formato)
    #print(tabla)
    #print(tabla_campo)
dfqvd1.head()



# ----------Captura de tabla_campo y campo ----------

# In[7]:


campo_var = pd.DataFrame(listas1,columns=["ValoresCampo","Cruza"])
#campo_var


# ----------Creacion un DataFrame que servirá como base para el .CSV final ----------

# In[8]:


dfGroup=pd.DataFrame({"Campo":[],"ValoresCampo":[],"Cruza":[],"ValoresValidacion":[]})
#dfGroup


# ----------Funcion creada para dar como no validas los registros que no cumplen con la extresion regular----------

# In[9]:


def asd (a):
    if a != "Valido":
        return("No valido")
    else:
        return(a)


# ----------Ciclo for en el que se agrupan por Campo, ValoresCampo Cruza y ValoresValidacion y se concatenan los resultados de cada formato requerido----------

# In[10]:


for i in range(len(dfDatos)):
    dfGroup11 = dfqvd1.groupby(["Campo",campo_var.iloc[i]["ValoresCampo"] ,campo_var.iloc[i]["Cruza"] ],as_index= False).agg({"ValoresValidacion":"count"})
    dfGroup11.rename(columns={"Campo":"Campo",campo_var.iloc[i]["ValoresCampo"]:"ValoresCampo",campo_var.iloc[i]["Cruza"]:"Cruza","ValoresValidacion":"ValoresValidacion"},inplace= True )
    dfGroup11["Campo"] = campo_var.iloc[i]["ValoresCampo"]   
    dfGroup = pd.concat([dfGroup,dfGroup11],axis=0)
    dfGroup["ValoresCampo"] = dfGroup["ValoresCampo"].replace("Nulo","")
    dfGroup["Cruza"] = dfGroup["Cruza"].apply(lambda a: asd(a))
dfGroup


# ----------Exportar a csv----------

# In[11]:


pathCSV = (dfRutaCSV.iloc[0]["Valor"]) 
pathCSV = pathCSV + "\\"  +tabla + ".csv"
#print(pathCSV)


# In[12]:


dfGroup.to_csv(pathCSV, index=False)


# ----------Mensaje final----------

# In[13]:


print("Archivo exportado, revisar ruta   " + pathCSV)

