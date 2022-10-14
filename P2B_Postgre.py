import psycopg2


def dbConectar():
    ip = "localhost"
    puerto = 5432
    basedatos = "Habitantes"

    usuario = "postgres"
    contrasena = "12345"

    print("---dbConectar---")
    print("---Conectando a Postgresql---")

    try:
        conexion = psycopg2.connect(user=usuario, password=contrasena, host=ip, port=puerto, database=basedatos)
        print("Conexión realizada a la base de datos", conexion)
        return conexion
    except psycopg2.DatabaseError as error:
        print("Error en la conexión")
        print(error)
        return None


# ------------------------------------------------------------------

def dbDesconectar():
    print("---dbDesconectar---")
    try:
        conexion.commit()
        conexion.close()
        print("Desconexión realizada correctamente")
        return True
    except psycopg2.DatabaseError as error:
        print("Error en la desconexión")
        print(error)
        return False


# ------------------------------------------------------------------

def dbMostrarPoblacion1():
    print("---dbMostrarPoblacion1---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion"
        cursor.execute(consulta)
        for tupla in cursor:
            print(tupla)
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error en dbMostrarPoblacion1")
        print(error)


# ------------------------------------------------------------------

def dbMostrarPoblacion2():
    print("---dbMostrarPoblacion2---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion"
        cursor.execute(consulta)
        tupla = cursor.fetchone()
        while tupla:
            print(tupla)
            tupla = cursor.fetchone()
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error en dbMostrarPoblacion2")
        print(error)


# ------------------------------------------------------------------

def dbMostrarPoblacion3():
    print("---dbMostrarPoblacion3---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion"
        cursor.execute(consulta)
        numTuplas = 5
        resul = cursor.fetchmany(numTuplas)
        for tupla in resul:
            print(tupla)
        print("Número de registros seleccionados:", len(resul))
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error en dbMostrarPoblacion3")
        print(error)


# ------------------------------------------------------------------

def dbMostrarPoblacion4():
    print("---dbMostrarPoblacion4---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion"
        cursor.execute(consulta)
        resul = cursor.fetchall()
        for tupla in resul:
            print(tupla)
        print("Número de registros recuperados:", len(resul))
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error en dbMostrarPoblacion4")
        print(error)


# ------------------------------------------------------------------

def dbObtenerPoblacion():
    print("---dbObtenerPoblacion---")

    # Por ejemplo, buscar Poblacion con codigo 987654321
    dniObjetivo = input("Introduce DNI de Poblacion: ")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion WHERE DNI = %s"
        cursor.execute(consulta, [dniObjetivo])
        resul = cursor.fetchall()
        for tupla in resul:
            print(tupla)
        print("Número de registros recuperados:", len(resul))
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
    except psycopg2.DatabaseError as error:
        print("Error. No se ha podido consultar al proyecto con código", dniObjetivo)
        print(error)


# ------------------------------------------------------------------

def dbConsultarPoblacion():
    print("---dbConsultarPoblacion---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Poblacion"
        cursor.execute(consulta)
        for tupla in cursor:
            print("DNI: ", tupla[0])
            print("NOMBRE: ", tupla[1])
            print("APELLIDO1: ", tupla[2])
            print("APELLIDO2: ", tupla[3])
            print("FNAC: ", tupla[4])
            print("DIRECCION: ", tupla[5])
            print("CP: ", tupla[6])
            print("SEXO: ", tupla[7])
            print("INGRESOS: ", tupla[8])
            print("GASTOS FIJOS: ", tupla[9])
            print("GASTOS ALIMENTACION:", tupla[10])
            print("GASTOS ROPA:", tupla[11])
            print("SECTOR:", tupla[12])
            print('------------------------------')
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error. No se han podido consultar los Poblacion")
        print(error)


# ------------------------------------------------------------------

def dbConsultarSectores():
    print("---dbConsultarSectores---")

    try:
        cursor = conexion.cursor()
        consulta = "SELECT * FROM Sectores"
        cursor.execute(consulta)
        for tupla in cursor:
            print("Codigo sector: ", tupla[0])
            print("Nombre Sector: ", tupla[1])
            print("Porcentaje sector: ", tupla[2])
            print("Ingresos sector: ", tupla[3])
            print('------------------------------')
        print("Número de registros recuperados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error. No se han podido consultar los Sectores")
        print(error)


# ------------------------------------------------------------------

def dbInsertarSectores():
    print("---dbInsertarSectores---")
    codigo = input("Codigo del sector:")
    nombre = input("Nombre del sector")
    porcentaje = 0.0
    ingresos = 0.0
    try:
        cursor = conexion.cursor()
        consulta = "INSERT INTO Sectores VALUES(%s, %s, %s, %s)"
        cursor.execute(consulta, [codigo, nombre, porcentaje, ingresos])
        print("Tupla insertada con éxito")
    except psycopg2.DatabaseError as error:
        print("Error. No se ha podido insertar el sector")
        print(error)


# ------------------------------------------------------------------


def dbModificarSectores():
    print("---dbModificarSectores---")
    nombre = "Desconocido"
    try:
        cursor = conexion.cursor()
        consulta = "UPDATE Sectores SET NOMBRES = %s WHERE cods = 0"
        cursor.execute(consulta, [nombre])
        print("Tupla/s modificada/s con éxito")
    except psycopg2.DatabaseError as error:
        print("Error. No se ha podido modificar el sector")
        print(error)


# ------------------------------------------------------------------

def dbBorrarSectores():
    print("---dbBorrarSectores---")
    cod = 0
    try:
        cursor = conexion.cursor()
        consulta = "DELETE FROM Sectores WHERE cods = %s"
        cursor.execute(consulta, [cod])
        print("Tupla borrada con éxito")
    except psycopg2.DatabaseError as error:
        print("Error. No se ha podido borrar el sector")
        print(error)


# ------------------------------------------------------------------

def dbInsertarMultiplesSectores():
    print("---dbInsertarMultiplesSectores---")

    datos = [
        ('5', 'Deportes', 0.0, 0.0),
        ('6', 'Autónomo', 0.0, 0.0),
        ('7', 'Tecnológicas', 0.0, 0.0)
    ]

    try:
        cursor = conexion.cursor()
        cursor.executemany("INSERT INTO Sectores VALUES(%s, %s, %s, %s)", datos)
        print("Número de registros insertados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error. No se han podido insertar múltipes Sectores")
        print(error)


# ------------------------------------------------------------------

def dbBorrarMultiplesSectores():
    print('---dbBorrarMultiplesSectores---')

    datos = [['5'], ['6'], ['7']]

    try:
        cursor = conexion.cursor()
        cursor.executemany("DELETE FROM Sectores WHERE cods = %s", datos)
        print("Número de registros borrados:", cursor.rowcount)
        print('------------------------------')
        cursor.close()
    except psycopg2.DatabaseError as error:
        print("Error. No se han podido borrar múltiples Sectores")
        print(error)


# ------------------------------------------------------------------
# ------------------------------------------------------------------
# ------------------------------------------------------------------

print("---Programa principal---")

conexion = dbConectar()

if (conexion is None):
    print("ERROR DE CONEXIÓN")
else:
    print("CONEXIÓN REALIZADA")

    dbMostrarPoblacion1()
    dbMostrarPoblacion2()
    dbMostrarPoblacion3()
    dbMostrarPoblacion4()

    dbObtenerPoblacion()
    dbConsultarPoblacion()
    dbConsultarSectores()

    dbInsertarSectores()
    dbConsultarSectores()

    dbModificarSectores()
    dbConsultarSectores()

    dbBorrarSectores()
    dbConsultarSectores()

    dbInsertarMultiplesSectores()
    dbConsultarSectores()

    dbBorrarMultiplesSectores()
    dbConsultarSectores()

    dbDesconectar()

print("---Fin de programa---")