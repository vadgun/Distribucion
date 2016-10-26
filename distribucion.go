package main 

import (
	"fmt"
	"strconv"
    "database/sql"
  _ "github.com/lib/pq"
   "github.com/senseyeio/roger"
)
//############################################################## F U N C I O N E S ###########################################################################

func conectardb(usuario string,password string,basededatos string,host string,port string,ssl string)*sql.DB{
	db, err := sql.Open(""+usuario+"", "user="+usuario+" password="+password+" dbname="+basededatos+" host="+host+" port="+port+" sslmode="+ssl+"")
	if err != nil {
		fmt.Println(err)
	}
return db
}

func dropandcreatetables(db *sql.DB){
//ELIMINA TABLAS
	_ , _ = db.Exec("DROP TABLE promedio")
	_ , _ = db.Exec("DROP TABLE bodega")
 	_ , _ = db.Exec("DROP TABLE velocidad")
 	_ , _ = db.Exec("DROP TABLE fallos")
//CREA TABLAS
	_ , _ = db.Exec("CREATE TABLE promedio()")
	_ , _ = db.Exec("CREATE TABLE bodega()")
	_ , _ = db.Exec("CREATE TABLE velocidad()")
	_ , _ = db.Exec("CREATE TABLE fallos()")
//CAMPOS INICIALES
	_ , _ = db.Exec("ALTER TABLE promedio ADD COLUMN sku VARCHAR(20)")
	_ , _ = db.Exec("ALTER TABLE bodega ADD COLUMN sku VARCHAR(20)")
	_ , _ = db.Exec("ALTER TABLE velocidad ADD COLUMN sku VARCHAR(20)")
	_ , _ = db.Exec("ALTER TABLE fallos ADD COLUMN sku VARCHAR(20)")
	_ , _ = db.Exec("ALTER TABLE fallos ADD COLUMN fallos NUMERIC")
	_ , _ = db.Exec("ALTER TABLE fallos ADD COLUMN porcentaje NUMERIC")
}

func contarfilas(db *sql.DB,tabla string) int{
	var filas_existentes int = 0
	filas, err := db.Query("SELECT COUNT(*)FROM "+tabla+"")
	if err != nil {
		fmt.Println(err)
	}
	for filas.Next(){
		err := filas.Scan(&filas_existentes)
		if err != nil{
			fmt.Println(err)
		}
	}
return filas_existentes
}

func contarcolumnas(db *sql.DB, tabla string) int {
	var columnas_existentes int = 0
	columnas, err := db.Query("SELECT COUNT(*)FROM information_schema.columns WHERE table_name = '"+tabla+"'")
	if err != nil {
		fmt.Println(err)
	}
	for columnas.Next(){
		err := columnas.Scan(&columnas_existentes)
		if err != nil{
 			fmt.Println(err)
		}
	}
return columnas_existentes	
}

func extraecolumnas(db *sql.DB, tabla string)[]string{
	var cadenas[] string
	var cadena string
	datos, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_name = '"+tabla+"'")
	if err != nil {
	fmt.Println(err)
	}
	for datos.Next(){
		err := datos.Scan(&cadena)
		cadenas = append(cadenas,cadena)
		if err != nil{
			fmt.Println(err)
		}
	}
	return cadenas
}

func extraeskus(db *sql.DB, tabla string)[]string{
	var cadenas[] string
	var cadena string
	datos, err := db.Query("SELECT sku FROM "+tabla+"")
	if err != nil {
	fmt.Println(err)
	}
	for datos.Next(){
		err := datos.Scan(&cadena)
		cadenas = append(cadenas,cadena)
		if err != nil{
			fmt.Println(err)
		}
	}
	return cadenas
}
func modificartablas(db *sql.DB,skusin,columsin string){
	_,_ = db.Exec("INSERT INTO promedio (sku) VALUES ('"+skusin+"')")
	_,_ = db.Exec("INSERT INTO bodega (sku) VALUES ('"+skusin+"')")
	_,_ = db.Exec("INSERT INTO velocidad (sku) VALUES ('"+skusin+"')")
	_,_ = db.Exec("ALTER TABLE promedio ADD COLUMN "+columsin+" NUMERIC")
	_,_ = db.Exec("ALTER TABLE bodega ADD COLUMN "+columsin+" NUMERIC")
	_,_ = db.Exec("ALTER TABLE velocidad ADD COLUMN "+columsin+" NUMERIC")
	
}

func updatetablas(db *sql.DB,skusin,columsin,promedio,bodega,velocidad string){
	_,_ = db.Exec("UPDATE promedio SET "+columsin+" = '"+promedio+"' WHERE sku= '"+skusin+"'")
	_,_ = db.Exec("UPDATE bodega SET "+columsin+" = '"+bodega+"' WHERE sku= '"+skusin+"'")
	_,_ = db.Exec("UPDATE velocidad SET "+columsin+" = '"+velocidad+"' WHERE sku= '"+skusin+"'")	
}

func promedioanterior(db *sql.DB,columna,skusin string) float64 {
	var promedio float64 = 0.0
	tabla_promedio,err := db.Query("SELECT "+columna+" FROM promedio where sku='"+skusin+"'")
		if err != nil{
			fmt.Println(err)
		}
		
		for tabla_promedio.Next(){
		err := tabla_promedio.Scan(&promedio)
		if err != nil{
			fmt.Println(err)
		}
	}
	return promedio
}

func bodegaanterior(db *sql.DB,columna,skusin string) float64{
	var bodega float64 = 0.0 
	tabla_bodega,err := db.Query("SELECT "+columna+" FROM bodega where sku='"+skusin+"'")
		if err != nil{
			fmt.Println(err)
		}						
		for tabla_bodega.Next(){
		err := tabla_bodega.Scan(&bodega)
		if err != nil{
			fmt.Println(err)
		}
	}
	return bodega
}

func altertablenextvalue(db *sql.DB,columna string) {
	_ , _ = db.Exec("ALTER TABLE promedio ADD COLUMN "+columna+" NUMERIC")
	_ , _ = db.Exec("ALTER TABLE bodega ADD COLUMN "+columna+" NUMERIC")
	_ , _ = db.Exec("ALTER TABLE velocidad ADD COLUMN "+columna+" NUMERIC")
}


func updatetablenextvalue(db *sql.DB,columna,sku,promedio,bodega,velocidad string){
	_,_ = db.Exec("UPDATE promedio SET "+columna+" = '"+promedio+"' WHERE sku= '"+sku+"'")
	_,_ = db.Exec("UPDATE bodega SET "+columna+" = '"+bodega+"' WHERE sku= '"+sku+"'")							
	_,_ = db.Exec("UPDATE velocidad SET "+columna+" = '"+velocidad+"' WHERE sku= '"+sku+"'")
}

func updatetablenextvaluepoisson(db *sql.DB,columna,sku,promedio,bodega,velocidad string) {
	_,_ = db.Exec("UPDATE promedio SET "+columna+" = '"+promedio+"' WHERE sku= '"+sku+"'")
	_,_ = db.Exec("UPDATE bodega SET "+columna+" = '"+bodega+"' WHERE sku= '"+sku+"'")
	_,_ = db.Exec("UPDATE velocidad SET "+columna+" = '"+velocidad+"' WHERE sku= '"+sku+"'")
}

func updatetablenextvaluebandera(db *sql.DB,columna,sku,promedio,bodega,velocidad string) {
	_,_ = db.Exec("UPDATE promedio SET "+columna+" = '"+promedio+"' WHERE sku= '"+sku+"'")
	_,_ = db.Exec("UPDATE bodega SET "+columna+" = '"+bodega+"' WHERE sku= '"+sku+"'")
	_,_ = db.Exec("UPDATE velocidad SET "+columna+" = '"+velocidad+"' WHERE sku= '"+sku+"'")	
}
func insertafallos(db *sql.DB,sku,fallos,porcentaje string){
	_,_ = db.Exec("INSERT INTO fallos (sku,fallos,porcentaje) VALUES ('"+sku+"','"+fallos+"','"+porcentaje+"')")
}


//#################################################################### M A I N ################################################################################
func main() {
//VARIABLES INICIALIZADAS
	//VARIABLES DEL USUARIO
	var calidad_usuario float64 = 0.0
	var dias_stock float64 = 0.0
	var seguridad_inicial float64 = 0.0
	var metodo int = 0
	//VARIABLES DE LAS BASES DE DATOS
	var filas int = 0
	var columnas int = 0
	//VARIABLES DE CALCULO
	var contador_promedio float64 = 0.0
	var promedio_inicial float64 = 0.0
	var bodega_inicial float64 = 0.0
	var bodega_dia float64 = 0.0
	var velocidad float64 = 0.0
	var venta_real float64 = 0.0
	var promedio_anterior float64 = 0.0
	var promedio_siguiente float64 = 0.0
	var bodega_con_venta float64 = 0.0
	var velocidad_actual float64 = 0.0
	var bodega_anterior float64 = 0.0
	var bandera_poisson bool = false
	var calidad_usuario_a_string,media string
	var pedidos_automaticos int = 0
	var pedidos_manuales int = 0
	var velocidad_poisson,bodega_poisson,promedio_poisson float64
	var multiplicacion float64 = 0.0
	var fallos_detectados float64 = 0.0
	var skusin,columsin string
//CONEXCION A R
	rClient, err := roger.NewRClient("127.0.0.1", 6311)
	if err != nil {
		fmt.Println("No se encontro el servidor de R" )
		return 
	}
	
//BASES DE DATOS   (usuario,contraseña,db,ip,puerto,sslmode)
	dblocal := conectardb("postgres","12345","real","localhost","5432","disable")
	dbventas := conectardb("postgres","pgBd2016","pruebas","192.168.1.191","5432","disable")
	defer dblocal.Close()
	defer dbventas.Close()

//TABLA ORIGEN
	var tabla_ventas string = "ventas_reales_graf"

//TABLAS
	dropandcreatetables(dblocal)

//PEDIR DATOS
	fmt.Print("  Introduce        Calidad Poisson   0 - 100 % => ")
	fmt.Scanf("%f\n",&calidad_usuario)
	fmt.Print("  Introduce        Dias stock                  => ")
	fmt.Scanf("%f\n",&dias_stock)
	fmt.Print("  Introduce        Seguridad inicial 0 - 100 % => ")
	fmt.Scanf("%f\n",&seguridad_inicial)
	fmt.Print("  Metodos :       Manual..0    Automatico..1   => ")
	fmt.Scanf("%d\n",&metodo)

//PORCENTAJES
	seguridad_inicial /= 100 
	calidad_usuario /= 100
	calidad_usuario_a_string = strconv.FormatFloat(calidad_usuario, 'f', 6, 64)

//CONOCER FILAS y COLUMNAS
	filas = contarfilas(dbventas,tabla_ventas)
	columnas = contarcolumnas(dbventas,tabla_ventas)
//EXTRAER NOMBRE DE SKUS Y COLS
	cols := extraecolumnas(dbventas,tabla_ventas)
	skus := extraeskus(dbventas,tabla_ventas)

if metodo == 1 {
	for f := 0; f < filas; f++{
		contador_promedio = 1
		fallos_detectados = 0.0
		for c := 1 ; c < columnas; c++{
			skusin = skus[f]
			columsin = cols[c]
			numero , _ := dbventas.Query("SELECT "+columsin+" FROM "+tabla_ventas+" where sku ='"+skusin+"'")
				
				for numero.Next(){
					err := numero.Scan(&venta_real)
					if err != nil{
					fmt.Println(err)
					}
				}
				if contador_promedio == 1{
					if venta_real == 0 {
						venta_real = 1.0
					}
					promedio_inicial = float64(venta_real)/float64(contador_promedio)
					bodega_inicial = (float64(venta_real)*float64(dias_stock))+(float64(venta_real)*float64(dias_stock)*float64(seguridad_inicial))
					bodega_dia = float64(bodega_inicial) - float64(venta_real) 
					velocidad = float64(bodega_inicial)/float64(promedio_inicial)

					promedio_convertido_a_string := strconv.FormatFloat(promedio_inicial,'f',6,64) 
					bodega_dia_convertido_a_string := strconv.FormatFloat(bodega_dia,'f',6,64) 
					velocidad_convertido_a_string := strconv.FormatFloat(velocidad,'f',6,64)
			
					modificartablas(dblocal,skusin,columsin)
					updatetablas(dblocal,skusin,columsin,promedio_convertido_a_string,bodega_dia_convertido_a_string,velocidad_convertido_a_string)
					contador_promedio ++ 
				}else{ // if contador_promedio == 1
					columna_anterior := cols[c-1]
					promedio_anterior = promedioanterior(dblocal,columna_anterior,skusin)
					bodega_anterior = bodegaanterior(dblocal,columna_anterior,skusin)

					promedio_siguiente = ((float64(promedio_anterior)*(float64(contador_promedio)-1.0)) + float64(venta_real))/float64(contador_promedio) 
					bodega_con_venta = float64(bodega_anterior) - float64(venta_real)
					velocidad_actual = float64(bodega_anterior)/float64(promedio_siguiente)
					altertablenextvalue(dblocal,columsin)

					if bodega_con_venta < 0 && bandera_poisson == false{
						fallos_detectados++
						}

					if velocidad_actual >= dias_stock{
							promedio_siguiente_convertido_a_string := strconv.FormatFloat(promedio_siguiente, 'f', 6, 64)
							bodega_con_venta_convertido_a_string := strconv.FormatFloat(bodega_con_venta, 'f', 6, 64)
							velocidad_actual_convertido_a_string := strconv.FormatFloat(velocidad_actual, 'f', 6, 64)	
							updatetablenextvalue(dblocal,columsin,skusin,promedio_siguiente_convertido_a_string,bodega_con_venta_convertido_a_string,velocidad_actual_convertido_a_string)	
					
					}else if velocidad_actual < dias_stock && bandera_poisson == false {
						var value interface{}
						pedidos_automaticos++
						media = strconv.FormatFloat(promedio_siguiente, 'f', 6, 64)
						value, _ = rClient.Eval("qpois("+calidad_usuario_a_string+","+media+")")
						multiplicacion = value.(float64) * dias_stock
						promedio_poisson = promedio_siguiente
						bodega_poisson = bodega_con_venta
						velocidad_poisson = velocidad_actual
						promedio_poisson_convertido_a_string := strconv.FormatFloat(promedio_poisson, 'f', 6, 64)
						bodega_poisson_convertido_a_string := strconv.FormatFloat(bodega_poisson, 'f', 6, 64)
						velocidad_poisson_convertido_a_string := strconv.FormatFloat(velocidad_poisson, 'f', 6, 64)
						updatetablenextvaluepoisson(dblocal,columsin,skusin,promedio_poisson_convertido_a_string,bodega_poisson_convertido_a_string,velocidad_poisson_convertido_a_string)
						bandera_poisson = true
						contador_promedio = 1
					}else{
						promedio_bandera := venta_real
				 		bodega_bandera := multiplicacion - venta_real
				 		velocidad_bandera := float64(bodega_anterior) / float64(venta_real)
				 		promedio_bandera_convertido_a_string := strconv.FormatFloat(promedio_bandera, 'f', 6, 64)
					 	bodega_bandera_convertido_a_string := strconv.FormatFloat(bodega_bandera, 'f', 6, 64)
					 	velocidad_bandera_convertido_a_string := strconv.FormatFloat(velocidad_bandera, 'f', 6, 64)	
					 	updatetablenextvaluebandera(dblocal,columsin,skusin,promedio_bandera_convertido_a_string,bodega_bandera_convertido_a_string,velocidad_bandera_convertido_a_string)			
						bandera_poisson = false
						contador_promedio = 1
					} //else de else if vel < d && poisson false
				}// else de // if contador_promedio == 1
				contador_promedio++
		} // for columnas
			columnas_existentes_float := float64(columnas)
		 	porcentaje := (fallos_detectados * 100) / columnas_existentes_float
		 	fallos_detectados_to_string := strconv.FormatFloat(fallos_detectados, 'f', 6, 64)
		 	porcentaje_to_string := strconv.FormatFloat(porcentaje, 'f', 6, 64)	
		 	insertafallos(dblocal,skusin,fallos_detectados_to_string,porcentaje_to_string)
	}//for filas
		fmt.Println("\n\nProceso automatico terminado....")
		fmt.Println("Total de pedidos automaticos...", pedidos_automaticos)
 	}// end metodo == 1 AUTOMATICO
if metodo == 0 {
for f := 0; f < filas; f++{
		contador_promedio = 1
		fallos_detectados = 0.0
		for c := 1 ; c < columnas; c++{
			skusin = skus[f]
			columsin := cols[c]
			numero , _ := dbventas.Query("SELECT "+columsin+" FROM ventas_reales_graf where sku ='"+skusin+"'")
				
				for numero.Next(){
					err := numero.Scan(&venta_real)
					if err != nil{
					fmt.Println(err)
					}
				}
				if contador_promedio == 1{
					if venta_real == 0 {
						venta_real = 1.0
					}
					promedio_inicial = float64(venta_real)/float64(contador_promedio)
					bodega_inicial = (float64(venta_real)*float64(dias_stock))+(float64(venta_real)*float64(dias_stock)*float64(seguridad_inicial))
					bodega_dia = float64(bodega_inicial) - float64(venta_real) 
					velocidad = float64(bodega_inicial)/float64(promedio_inicial)

					promedio_convertido_a_string := strconv.FormatFloat(promedio_inicial,'f',6,64) 
					bodega_dia_convertido_a_string := strconv.FormatFloat(bodega_dia,'f',6,64) 
					velocidad_convertido_a_string := strconv.FormatFloat(velocidad,'f',6,64)
			
					modificartablas(dblocal,skusin,columsin)
					updatetablas(dblocal,skusin,columsin,promedio_convertido_a_string,bodega_dia_convertido_a_string,velocidad_convertido_a_string)
					contador_promedio ++ 
				}else{ // if contador_promedio == 1
					columna_anterior := cols[c-1]
					promedio_anterior = promedioanterior(dblocal,columna_anterior,skusin)
					bodega_anterior = bodegaanterior(dblocal,columna_anterior,skusin)

					promedio_siguiente = ((float64(promedio_anterior)*(float64(contador_promedio)-1.0)) + float64(venta_real))/float64(contador_promedio) 
					bodega_con_venta = float64(bodega_anterior) - float64(venta_real)
					velocidad_actual = float64(bodega_anterior)/float64(promedio_siguiente)
					altertablenextvalue(dblocal,columsin)

					if bodega_con_venta < 0 && contador_promedio != dias_stock{
						fallos_detectados++
					}

					if contador_promedio < dias_stock{
							promedio_siguiente_convertido_a_string := strconv.FormatFloat(promedio_siguiente, 'f', 6, 64)
							bodega_con_venta_convertido_a_string := strconv.FormatFloat(bodega_con_venta, 'f', 6, 64)
							velocidad_actual_convertido_a_string := strconv.FormatFloat(velocidad_actual, 'f', 6, 64)	
							updatetablenextvalue(dblocal,columsin,skusin,promedio_siguiente_convertido_a_string,bodega_con_venta_convertido_a_string,velocidad_actual_convertido_a_string)	
					
					}else if contador_promedio == dias_stock && velocidad_actual < dias_stock && bandera_poisson == false {
						var value interface{}
						pedidos_manuales++
						media = strconv.FormatFloat(promedio_siguiente, 'f', 6, 64)
						value, _ = rClient.Eval("qpois("+calidad_usuario_a_string+","+media+")")
						multiplicacion = value.(float64) * dias_stock
						promedio_poisson = promedio_siguiente
						bodega_poisson = bodega_con_venta
						velocidad_poisson = velocidad_actual
						promedio_poisson_convertido_a_string := strconv.FormatFloat(promedio_poisson, 'f', 6, 64)
						bodega_poisson_convertido_a_string := strconv.FormatFloat(bodega_poisson, 'f', 6, 64)
						velocidad_poisson_convertido_a_string := strconv.FormatFloat(velocidad_poisson, 'f', 6, 64)
						updatetablenextvaluepoisson(dblocal,columsin,skusin,promedio_poisson_convertido_a_string,bodega_poisson_convertido_a_string,velocidad_poisson_convertido_a_string)
						bandera_poisson = true
						contador_promedio = 1
					}else{
						promedio_bandera := venta_real
				 		bodega_bandera := multiplicacion - venta_real
				 		velocidad_bandera := float64(bodega_anterior) / float64(venta_real)
				 		promedio_bandera_convertido_a_string := strconv.FormatFloat(promedio_bandera, 'f', 6, 64)
					 	bodega_bandera_convertido_a_string := strconv.FormatFloat(bodega_bandera, 'f', 6, 64)
					 	velocidad_bandera_convertido_a_string := strconv.FormatFloat(velocidad_bandera, 'f', 6, 64)	
					 	updatetablenextvaluebandera(dblocal,columsin,skusin,promedio_bandera_convertido_a_string,bodega_bandera_convertido_a_string,velocidad_bandera_convertido_a_string)			
						bandera_poisson = false
						contador_promedio = 1
					} 
				}// else de // if contador_promedio == 1
				contador_promedio++
		} // for columnas
			columnas_existentes_float := float64(columnas)
		 	porcentaje := (fallos_detectados * 100) / columnas_existentes_float
		 	fallos_detectados_to_string := strconv.FormatFloat(fallos_detectados, 'f', 6, 64)
		 	porcentaje_to_string := strconv.FormatFloat(porcentaje, 'f', 6, 64)	
		 	insertafallos(dblocal,skusin,fallos_detectados_to_string,porcentaje_to_string)
	}//for filas
		fmt.Println("\n\nProceso automatico terminado....")
		fmt.Println("Total de pedidos manual...", pedidos_manuales)
 	}// end metodo == 0 MANUAL

}//end main
