from statistics import stdev
import pandas as pd
from pathlib import Path
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import chardet
from sqlalchemy import create_engine
import pymysql

########################################### INGESTA ###########################################

ds_path = Path('Datasets') # Path to the datasets folder

# Concatenar todos los datasets de venta dentro de uno
venta_list = []
for file in ds_path.glob('ven*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    venta_list.append(df)
df_venta = pd.concat(venta_list)

# Concatenar todos los datasets de compra dentro de uno
compra_list = []
for file in ds_path.glob('com*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    compra_list.append(df)
df_compra = pd.concat(compra_list)

# Concatenar todos los datasets de gasto dentro de uno
gasto_list = []
for file in ds_path.glob('gas*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    gasto_list.append(df)
df_gasto = pd.concat(gasto_list)

# Concatenar todos los datasets de cliente dentro de uno
cliente_list = []
for file in ds_path.glob('cli*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=';', decimal=',', encoding=codificacion)
    cliente_list.append(df)
df_cliente = pd.concat(cliente_list)

# Concatenar todos los datasets de localidad dentro de uno
localidad_list = []
for file in ds_path.glob('loc*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    localidad_list.append(df)
df_localidad = pd.concat(localidad_list)

# Concatenar todos los datasets de proveedor dentro de uno
proveedor_list = []
for file in ds_path.glob('prov*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    proveedor_list.append(df)
df_proveedor = pd.concat(proveedor_list)

# Concatenar todos los datasets de sucursal dentro de uno
sucursal_list = []
for file in ds_path.glob('suc*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=';', decimal=',', encoding=codificacion)
    sucursal_list.append(df)
df_sucursal = pd.concat(sucursal_list)

# Concatenar todos los datasets de producto dentro de uno
producto_list = []
for file in ds_path.glob('prod*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=';', decimal=',', encoding=codificacion)
    producto_list.append(df)
df_producto = pd.concat(producto_list)

# Concatenar todos los datasets de canal dentro de uno
canal_list = []
for file in ds_path.glob('can*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal=',', encoding=codificacion)
    canal_list.append(df)
df_canal = pd.concat(canal_list)

############################################# NORMALIZACION #############################################

# 1. Renombrar las columnas de cada tabla
df_venta.columns = ['id_venta', 'fecha', 'fecha_entrega', 'id_canal', 'id_cliente', 'id_sucursal', 'id_empleado', 'id_producto', 'precio', 'cantidad']
df_compra.columns = ['id_compra', 'fecha', 'fecha_anio', 'fecha_mes', 'fecha_periodo', 'id_producto', 'cantidad', 'precio', 'id_proveedor']
df_gasto.columns = ['id_gasto', 'id_sucursal', 'id_tipo_gasto', 'fecha', 'monto']
df_cliente.columns = ['id_cliente', 'provincia', 'nombre_apellido', 'domicilio', 'telefono', 'edad', 'localidad', 'longitud', 'latitud', 'col10']
df_localidad.columns = ['categoria', 'latitud', 'longitud', 'id_departamento', 'departamento', 'fuente', 'id_localidad', 'id_localidad_censal', 'localidad_censal', 'id_municipio', 'municipio', 'nombre', 'id_provincia', 'provincia']
df_proveedor.columns = ['id_proveedor', 'nombre', 'domicilio', 'localidad', 'provincia', 'pais', 'departamento']
df_sucursal.columns = ['id_sucursal', 'sucursal', 'domicilio', 'localidad', 'provincia', 'latitud', 'longitud']
df_producto.columns = ['id_producto', 'concepto', 'tipo_producto', 'precio']
df_canal.columns = ['id_canal', 'canal']

############## VENTA ###############

# 1. Buscar valores duplicados en la columna id_venta
if df_venta.duplicated(subset='id_venta').sum() > 0:
    print('Se han depurado', df_venta.duplicated().sum(), 'ventas duplicadas')
    # Eliminar los valores duplicados
    df_venta.drop_duplicates(subset='id_venta', keep='first', inplace=True)

# 2. Remplazar valores en precio de venta en funcion de la columna precio de la tabla producto
df_venta['precio'] = df_venta.apply(lambda row: df_producto.loc[df_producto['id_producto'] == row['id_producto']]['precio'].values[0], axis=1)

# 3. Remplazar los valores nulos en cantidad de venta por el valor correspondiente a la media
media_cant = round(df_venta.cantidad.mean())

df_venta['cantidad'] = df_venta.apply(lambda row: media_cant if pd.isnull(row['cantidad']) else row['cantidad'], axis=1)

# 4. Remplazar los valores mayores al techo de la deteccion de otliers por su media en la columna cantidad de venta
techo_cant = round(media_cant + (3 * stdev(df_venta.cantidad)))

df_venta['cantidad'] = df_venta.apply(lambda row: media_cant if row['cantidad'] > techo_cant or row['cantidad'] < 0 else row['cantidad'], axis=1)


############## COMPRA ###############

# 1. Buscar valores duplicados en la columna id_compra
if df_compra.duplicated(subset='id_compra').sum() > 0:
    print('Se han depurado', df_compra.duplicated().sum(), 'compras duplicadas')
    # Eliminar los valores duplicados
    df_compra.drop_duplicates(subset='id_compra', keep='first', inplace=True)

# 2. Remplazar los valores nulos en precio de compra en funcion de la columna precio de la tabla producto
df_compra['precio'] = df_compra.apply(lambda row: df_producto.loc[df_producto['id_producto'] == row['id_producto']]['precio'].values[0] if pd.isnull(row['precio']) else row['precio'], axis=1)

# 3. Remplazar los valores mayores al techo de la formulacion de outliers en la columna precio de compra por el precio de la tabla producto
media_prec_com = round(df_compra.precio.mean())
techo_prec_com = round(media_prec_com + (3 * stdev(df_compra.precio)))

df_compra['precio'] = df_compra.apply(lambda row: df_producto.loc[df_producto['id_producto'] == row['id_producto']]['precio'].values[0] if row['precio'] > techo_prec_com or row['precio'] < 0 else row['precio'], axis=1)

# 4. Remplazar los valores mayores a al techo de la deteccion de outliers en cantidad de compra por la media
media_cant_com = round(df_compra.cantidad.mean())
techo_cant_com = round(media_cant_com + (3 * stdev(df_compra.cantidad)))

df_compra['cantidad'] = df_compra.apply(lambda row: media_cant_com if row['cantidad'] > techo_cant_com or row['cantidad'] < 0 else row['cantidad'], axis=1)

# 5. Remplazar '/' por '-' en la columna fecha de compra
df_compra['fecha'] = df_compra.apply(lambda row: row['fecha'].replace('/', '-'), axis=1)

# 6. Crear la columna fecha_dia, con el valor entre '-' y '-' de la columna fecha de compra
df_compra['fecha_dia'] = df_compra.apply(lambda row: row['fecha'].split('-')[1], axis=1)

# 7. Crear la columna aux_fecha con los valores concatenados de fecha_anio, fecha_mes y fecha_dia
df_compra['fecha'] = df_compra.apply(lambda row: str(row['fecha_anio']) + '-' + str(row['fecha_mes']) + '-' + str(row['fecha_dia']), axis=1)

# 8. Convertir la columna fecha de compra a tipo datetime
df_compra['fecha'] = pd.to_datetime(df_compra['fecha'])


############## CLIENTE ###############

# 1. Buscar valores duplicados en la columna id_cliente
if df_cliente.duplicated(subset='id_cliente').sum() > 0:
    print('Se han depurado', df_cliente.duplicated().sum(), 'clientes duplicadas')
    # Eliminar los valores duplicados
    df_cliente.drop_duplicates(subset='id_cliente', keep='first', inplace=True)

# 2. Remplazar los valores nulos de las columnas nombre_apellido, provincia, domicilio, telefono y localidad por 'Sin Dato'
df_cliente['nombre_apellido'] = df_cliente.apply(lambda row: 'Sin Dato' if pd.isnull(row['nombre_apellido']) else row['nombre_apellido'], axis=1)
df_cliente['provincia'] = df_cliente.apply(lambda row: 'Sin Dato' if pd.isnull(row['provincia']) else row['provincia'], axis=1)
df_cliente['domicilio'] = df_cliente.apply(lambda row: 'Sin Dato' if pd.isnull(row['domicilio']) else row['domicilio'], axis=1)
df_cliente['telefono'] = df_cliente.apply(lambda row: 'Sin Dato' if pd.isnull(row['telefono']) else row['telefono'], axis=1)
df_cliente['localidad'] = df_cliente.apply(lambda row: 'Sin Dato' if pd.isnull(row['localidad']) else row['localidad'], axis=1)

# 3. Remplazar los valores nulos de las columnas latitud y longitu por 0
df_cliente['longitud'] = df_cliente.apply(lambda row: 0 if pd.isnull(row['longitud']) else row['longitud'], axis=1)
df_cliente['latitud'] = df_cliente.apply(lambda row: 0 if pd.isnull(row['latitud']) else row['latitud'], axis=1)


# 4. Multiplicar por menos uno valores positivos en las columnas de latitud y longitud de la tabla cliente
df_cliente['longitud'] = df_cliente.apply(lambda row: -1 * row['longitud'] if row['longitud'] > 0 else row['longitud'], axis=1)
df_cliente['latitud'] = df_cliente.apply(lambda row: -1 * row['latitud'] if row['latitud'] > 0 else row['latitud'], axis=1)


# 5. Eliminar la columna col10 de la tabla cliente
df_cliente.drop(columns=['col10'], inplace=True)

# 6. Normalizar la columna nombre_apellido, domicilio 
df_cliente.nombre_apellido = df_cliente.nombre_apellido.str.title()
df_cliente.domicilio = df_cliente.domicilio.str.title()

############## LOCALIDAD ###############

# 1. Buscar valores duplicados en la columna id_localidad
if df_localidad.duplicated(subset='id_localidad').sum() > 0:
    print('Se han depurado', df_localidad.duplicated().sum(), 'localidades duplicadas')
    # Eliminar los valores duplicados
    df_localidad.drop_duplicates(subset='id_localidad', keep='first', inplace=True)

# 2. Remplazar los valores nulos de las columnas categoricas por 'Sin Dato'
df_localidad['departamento'] = df_localidad.apply(lambda row: 'Sin Dato' if pd.isnull(row['departamento']) else row['departamento'], axis=1)

# 3. Tambine las variables numericas por 0
df_localidad['id_departamento'] = df_localidad.apply(lambda row: 0 if pd.isnull(row['id_departamento']) else row['id_departamento'], axis=1)
df_localidad['id_municipio'] = df_localidad.apply(lambda row: 0 if pd.isnull(row['id_municipio']) else row['id_municipio'], axis=1)

# 4. Normalizar la columna nombre 
df_localidad.nombre = df_localidad.nombre.str.title()

############## PROVEEDOR ###############

# 1. Buscar valores duplicados en la columna id_proveedor
if df_proveedor.duplicated(subset='id_proveedor').sum() > 0:
    print('Se han depurado', df_proveedor.duplicated().sum(), 'proveedores duplicadas')
    # Eliminar los valores duplicados
    df_proveedor.drop_duplicates(subset='id_proveedor', keep='first', inplace=True)

# 2. Remplazar los valores nulos de la columna nombre por 'Sin Dato'
df_proveedor['nombre'] = df_proveedor.apply(lambda row: 'Sin Dato' if pd.isnull(row['nombre']) else row['nombre'], axis=1)

# 3. Normalizar las columnas, domicilio, localidad, provincia, pais y departamento

df_proveedor.domicilio = df_proveedor.domicilio.str.title()
df_proveedor.localidad = df_proveedor.localidad.str.title()
df_proveedor.provincia = df_proveedor.provincia.str.title()
df_proveedor.pais = df_proveedor.pais.str.title()
df_proveedor.departamento = df_proveedor.departamento.str.title()

############## SUCURSAL ###############

# 1. Buscar valores duplicados en la columna id_sucursal
if df_sucursal.duplicated(subset='id_sucursal').sum() > 0:
    print('Hay', df_sucursal.duplicated().sum(), 'sucursals duplicadas')
    # Eliminar los valores duplicados
    df_sucursal.drop_duplicates(subset='id_sucursal', keep='first', inplace=True)

# 2. Remplazar los valores nulos de las columnas latitud y longitu por 0
df_sucursal['longitud'] = df_sucursal.apply(lambda row: 0 if pd.isnull(row['longitud']) else row['longitud'], axis=1)
df_sucursal['latitud'] = df_sucursal.apply(lambda row: 0 if pd.isnull(row['latitud']) else row['latitud'], axis=1)

# 3. Multiplicar por menos uno valores positivos en las columnas de latitud y longitud de la tabla sucursal
df_sucursal['longitud'] = df_sucursal.apply(lambda row: -1 * row['longitud'] if row['longitud'] > 0 else row['longitud'], axis=1)
df_sucursal['latitud'] = df_sucursal.apply(lambda row: -1 * row['latitud'] if row['latitud'] > 0 else row['latitud'], axis=1)

############## PRODUCTO ###############

# 1. Buscar valores duplicados en la columna id_producto
if df_producto.duplicated(subset='id_producto').sum() > 0:
    print('Se han depurado', df_producto.duplicated().sum(), 'productos duplicadas')
    # Eliminar los valores duplicados
    df_producto.drop_duplicates(subset='id_producto', keep='first', inplace=True)

# 2. Remplazar los valores nulos de la columna tipo_producto por 'Sin Dato'
df_producto['tipo_producto'] = df_producto.apply(lambda row: 'Sin Dato' if pd.isnull(row['tipo_producto']) else row['tipo_producto'], axis=1)

# 3. Normalizar las columnas, concepto y tipo_producto
df_producto.concepto = df_producto.concepto.str.title()
df_producto.tipo_producto = df_producto.tipo_producto.str.title()

############################################# EJERCICIO NORMALIZACION DE LOCALIDAD #############################################

# 1. Utilizar str.title() en la columna localidad de la tabla cliente y en localidad_censal de la tabla localidad

df_cliente.localidad = df_cliente.localidad.str.title()
df_localidad.localidad_censal = df_localidad.localidad_censal.str.title()

# 2. Usar fuzz y levenshtein para normalizar los valores unicos de la columna localidad de la tabla cliente en funcion de los valores unicos de la columna nombre de la tabla localidad

localidad = df_localidad.localidad_censal.value_counts().index
local_clientes_unique = df_cliente.localidad.unique()

normal = []

def get_match(query, choices):
    for i in query:
        tupla = process.extractOne(i, choices)
        normal.append(tupla[0])
    return normal

loc_cli_cor = get_match(local_clientes_unique, localidad)

m_dict = {local_clientes_unique[i]: loc_cli_cor[i] for i in range(0, len(local_clientes_unique))}

df_cliente['localidad_normal'] = df_cliente.localidad.map(m_dict)

# 3. Traer el id_localidad de la tabla localidad para cada localidad normalizada de la tabla cliente

df_cliente['id_localidad'] = df_cliente.apply(lambda row: df_localidad[df_localidad.localidad_censal == row['localidad_normal']].id_localidad.values[0] if row['localidad_normal'] in df_localidad.localidad_censal.values else 0, axis=1)

# 4. Llenar valores Sin Dato en la columna provincia de la tabla cliente con el valor de la columna provincia de la tabla localidad en funcion del id_localidad

df_cliente['provincia'] = df_cliente.apply(lambda row: df_localidad[df_localidad.id_localidad == row['id_localidad']].provincia.values[0] if row['id_localidad'] in df_localidad.id_localidad.values else 'Sin Dato', axis=1)

# 5. Completar valores en cero de las columnas latitud y longitud de la tabla cliente con los valores de la columna latitud y longitud de la tabla localidad en funcion del id_localidad

# 'DataFrame' object has no attribute 'longitud'
df_cliente['longitud'] = df_cliente.apply(lambda row: df_localidad[df_localidad.id_localidad == row['id_localidad']].longitud.values[0] if row['id_localidad'] in df_localidad.id_localidad.values else 0, axis=1)
df_cliente['latitud'] = df_cliente.apply(lambda row: df_localidad[df_localidad.id_localidad == row['id_localidad']].latitud.values[0] if row['id_localidad'] in df_localidad.id_localidad.values else 0, axis=1)

############################################# INGESTAR SQL #############################################

try:
    pymysql.install_as_MySQLdb()

    # 1. Usar la funcion to_sql para ingresar las tablas normalizadas en la base de datos

    engine = create_engine('mysql+mysqldb://root:root@localhost/proyecto_individual', echo = False)
    df_venta.to_sql(name='venta', con=engine, if_exists='append', index=False)
    df_compra.to_sql(name='compra', con=engine, if_exists='append', index=False)
    df_gasto.to_sql(name='gasto', con=engine, if_exists='append', index=False)
    df_cliente.to_sql(name='cliente', con=engine, if_exists='append', index=False)
    df_localidad.to_sql(name='localidad', con=engine, if_exists='append', index=False)
    df_proveedor.to_sql(name='proveedor', con=engine, if_exists='append', index=False)
    df_sucursal.to_sql(name='sucursal', con=engine, if_exists='append', index=False)
    df_producto.to_sql(name='producto', con=engine, if_exists='append', index=False)
    df_canal.to_sql(name='canal', con=engine, if_exists='append', index=False)

    print('Proceso terminado con MySQL.')

except:
    print('Proceso terminado sin MySQL')