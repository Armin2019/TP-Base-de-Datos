package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func createDatabase() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`DROP database if exists tp`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create database tp`)
	if err != nil {
		log.Fatal(err)
	}

}

func createTables() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create table cliente (nrocliente int,nombre text,apellido text,domicilio text,telefono char(12))`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table tarjeta(nrotarjeta char(16),nrocliente int,validadesde char(6),validahasta char(6),codseguridad char(4),limitecompra decimal(8,2),estado char(10))`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table comercio (nrocomercio int,nombre text,domicilio text,codigopostal char(8),telefono char(12))`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table compra (nrooperacion serial,nrotarjeta char(16),nrocomercio int,fecha timestamp,monto decimal(7,2),pagado boolean)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table rechazo (nrorechazo serial,nrotarjeta char(16),nrocomercio int,fecha timestamp,monto decimal(7,2),motivo text)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table cierre(año int,mes int,terminacion int,fechainicio date,fechacierre date,fechavto date)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table cabecera(nroresumen serial,nombre text,apellido text,domicilio text,nrotarjeta char(16),desde date,hasta date,vence date,total decimal(8,2))`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table detalle(nroresumen int,nrolinea serial,fecha date,nombrecomercio text,monto decimal(7,2))`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table alerta(nroalerta serial,nrotarjeta char(16),fecha timestamp,nrorechazo int,codalerta int,descripcion text)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table consumo(nrotarjeta char(16),codseguridad char(14),nrocomercio int,monto decimal(7,2))`)
	if err != nil {
		log.Fatal(err)
	}

}

func agregarClaves() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`alter table cliente add constraint cliente_pk primary key (nrocliente)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table tarjeta add constraint tarjeta_pk primary key (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table comercio add constraint comercio_pk primary key (nrocomercio)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table compra add constraint compra_pk primary key (nrooperacion)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table rechazo add constraint rechazo_pk primary key (nrorechazo)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table cierre add constraint cierre_pk primary key (año, mes, terminacion)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table cabecera add constraint cabecera_pk primary key (nroresumen)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table detalle add constraint detalle_pk primary key (nroresumen, nrolinea)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table alerta add constraint alerta_pk primary key (nroalerta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table tarjeta add constraint tarjeta_nrocliente_fk foreign key (nrocliente) references cliente (nrocliente)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table compra add constraint compra_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table compra add constraint compra_nrocomercio_fk foreign key (nrocomercio) references comercio (nrocomercio)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table rechazo add constraint rechazo_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table rechazo add constraint rechazo_nrocomercio_fk foreign key (nrocomercio) references comercio (nrocomercio)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table cabecera add constraint cabecera_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table detalle add constraint cabecera_nroresumen_fk foreign key (nroresumen) references cabecera (nroresumen)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table alerta add constraint alerta_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table alerta add constraint alerta_nrorechazo_fk foreign key (nrorechazo) references rechazo (nrorechazo)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table consumo add constraint consumo_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta (nrotarjeta)`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`alter table consumo add constraint consumo_nrocomercio_fk foreign key (nrocomercio) references comercio (nrocomercio)`)
	if err != nil {
		log.Fatal(err)
	}
}

func agregarRegistros() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	insert into comercio values (1,'aluar','pasteur 4600','B1644AMV','47258000');
	insert into comercio values (2,	'Central Puerto','Av. Tomas Alva Edison','B1644AMV','43175000');
	insert into comercio values (3,'Sociedad Comercial del Plata','Colectora Panamericana 1804','B1607EEV','1121526000');
	insert into comercio values (4,'Cresud','Carlos M.Della Paolera 26','C1001ADA','48147800');
	insert into comercio values (5,'Holcim','Humberto Primo 680','X5000FAN','08007776463');
	insert into comercio values (6,'Loma Negra','Cecilia Grienson 355','C1107CPG','43193000');
	insert into comercio values (7,'Mirgor','Miñones 2177','C1428ATG','1137527100');
	insert into comercio values (8,'Pampa Energia','Maipu ','C1084ABA','43446000');
	insert into comercio values (9,'Transportadora Gas del Norte','Don Bosco 3672','C1206ABF','40082000');
	insert into comercio values (10,'Ternium','Av. Leandro Alem 1067','C1001AAF','40182100');
	insert into comercio values (11,'Carlos Casado','Av. Leandro Alem 855','C1001AAF','43110170');
	insert into comercio values (12,'Capex','Carlos F. Melo 630','B1638CHB','4796600');
	insert into comercio values (13,'Celulosa Argentina','Av. Santa Fe 1821','C1123AAA','32219300');
	insert into comercio values (14,'Ferrum','España 496','B1870BWJ','08002222266');
	insert into comercio values (15,'Fiplasto','Adolfo Alsina 756','C1087AAL','51713000');
	insert into comercio values (16,'Dos Ancla','Chile778','C1098AAP','08002224476');
	insert into comercio values (17,'Inversora Juramento','Ruta Nac. 16 km16','A4448XDA','3877247058');
	insert into comercio values (18,'Ledesma','Av. Corrientes 415','C1043AAE','43781555');
	insert into comercio values (19,'Morixe','Esmeralda 1320','C1007ABR','08003336674');
	insert into comercio values (20,'Richmond','Bouchard 680','C1106ABJ','5551600');`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`insert into cliente values (1,'Jose','San Martin','Pujol 1960','11-3958-0889');
	insert into cliente values (2,'Lionel','Messi','Eiffel 1987','11-4714-4876');
	insert into cliente values (3,'Pedro','Perez','Marquez 6875','11-5873-5867');
	insert into cliente values (4,'Monica','Morales','Moreno 6874','11-5874-3629');
	insert into cliente values (5,'Silvina','Sanchez','Suipacha 687','11-6875-9874');
	insert into cliente values (6,'Joaquin','Juarez','Jamaica 6987','11-1998-7685');
	insert into cliente values (7,'Alvaro','Alves','Kennedy 5674','11-3455-6783');
	insert into cliente values (8,'Bruno','Diaz','Gutierrez 4410','11-8769-0309');
	insert into cliente values (9,'Leandro','Lopez','Uriburu 798','11-9089-5546');
	insert into cliente values (10,'Diego','Dominguez','9 de Julio 955','11-9876-5467');
	insert into cliente values (11,'Arturo','Acuña','San Luis 7580','11-2435-5469');
	insert into cliente values (12,'Claudia','Correa','Eva Peron 8679','11-1919-1952');
	insert into cliente values (13,'Cecilia','Arana','Maipu 9867','11-2002-6833');
	insert into cliente values (14,'Santiago','Soler','Bolivia 7869','11-5478-1231');
	insert into cliente values (15,'German','Gomez','Av. de Mayo 6123','11-1298-6566');
	insert into cliente values (16,'Sofia','Leiva','Sucre 1675','11-9947-2006');
	insert into cliente values (17,'Viviana','Veron','Ureña 1222','11-6623-7689');
	insert into cliente values (18,'Mercedes','Mendez','Rosario 1765','11-6546-7787');
	insert into cliente values (19,'Dario','Diaz','Sanabria 6909','11-2601-1662');
	insert into cliente values (20,'Benjamin','Fernandez','Rolon 155','11-9963-3952');`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
	insert into tarjeta values ('4424586923689485',1,'202108','202308','9867',40000.0,'vigente');
	insert into tarjeta values ('4268576829345681',1,'201903','202201','0978',30000.05,'suspendida');
	insert into tarjeta values ('4244985547281001',2,'202201','202401','1238',100000.99,'anulada');
	insert into tarjeta values ('4486955738695342',3,'202108','202909','0050',150000.0,'vigente');
	insert into tarjeta values ('4255986729685839',4,'202009','202210','8852',60000.98,'vigente');
	insert into tarjeta values ('5869275649284758',5,'202203','202501','9442',200000.67,'vigente');
	insert into tarjeta values ('5547858847289502',6,'201908','202511','5808',500000.56,'vigente');
	insert into tarjeta values ('4424985726385716',7,'202002','202304','9992',75000.55,'vigente');
	insert into tarjeta values ('5578273615690100',7,'202009','202302','7764',80000.0,'vigente');
	insert into tarjeta values ('5574829175628591',8,'202112','202401','7890',100000.98,'vigente');
	insert into tarjeta values ('5587216756123989',9,'202009','202306','1236',95000.67,'vigente');
	insert into tarjeta values ('4286175688279018',10,'202006','202209','6766',60000.78,'vigente');
	insert into tarjeta values ('5598687129867123',11,'202105','202501','0098',50000.00,'vigente');
	insert into tarjeta values ('5512867591857129',12,'202107','202403','0908',98000.89,'vigente');
	insert into tarjeta values ('4297817681009187',13,'202012','202309','0226',100000.97,'vigente');
	insert into tarjeta values ('5577186756128491',14,'202009','202401','5445',350000.98,'vigente');
	insert into tarjeta values ('4200971238756119',15,'202107','202407','6901',600000.78,'vigente');
	insert into tarjeta values ('5566172845123512',16,'201812','202209','3368',90000.0,'vigente');
	insert into tarjeta values ('5598867159001899',17,'202009','202308','2198',110000.98,'vigente');
	insert into tarjeta values ('4296756186918512',18,'202007','202401','7701',130000.87,'vigente');
	insert into tarjeta values ('4478902867162199',19,'201911','202210','9812',400000.99,'vigente');
	insert into tarjeta values ('5511586719928671',20,'202010','202104','6788',550000.0,'vigente');`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
	
	insert into consumo values('4424586923689485', '9867', 1, 40.10); --Prueba de compra válida
	insert into consumo values('4424586923689485', '9867', 1, 70.10); --Prueba de compra válida 
	insert into consumo values('4424586923689485', '9867', 2, 100.50); --Prueba de compra en distintos comercios con el mismo código postal

	insert into consumo values ('4486955738695342', '0050', 3, 800.90);
	insert into compra values (default,'4486955738695342',4, current_timestamp + interval '4 minute', 60.20, false); --Prueba de compra en menos de 5 minutos en comercios con diferentes códigos postales
		
	insert into consumo values('4424586923689485', '9861', 1, 40.10); --Prueba de tarjeta con código erroneo
	insert into consumo values('4244985547281001', '1238', 1, 31.10); --Prueba de tarjeta anulada
	insert into consumo values('4268576829345681', '0978', 1, 61.10); --Prueba de tarjeta suspendida
	insert into consumo values('5511586719928671', '6788', 1, 70.10); --Prueba de tarjeta vencida

	insert into consumo values('4424586923689485', '9867', 1, 50000.20);
	insert into consumo values('4424586923689485', '9867', 1, 50000.20); --Prueba de rechazo por exceso de límite dos veces, la tarjeta pasa a suspendida
	`)
	if err != nil {
		log.Fatal(err)
	}

}

func dropKeys() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
					 alter table tarjeta drop constraint tarjeta_nrocliente_fk;
					 alter table compra drop constraint compra_nrotarjeta_fk;
					 alter table compra drop constraint compra_nrocomercio_fk;
					 alter table rechazo drop constraint rechazo_nrotarjeta_fk;
					 alter table rechazo drop constraint rechazo_nrocomercio_fk;
					 alter table cabecera drop constraint cabecera_nrotarjeta_fk;
					 alter table detalle drop constraint cabecera_nroresumen_fk;
					 alter table alerta drop constraint alerta_nrotarjeta_fk;
					 alter table alerta drop constraint alerta_nrorechazo_fk;
					 alter table consumo drop constraint consumo_nrotarjeta_fk;
					 alter table consumo drop constraint consumo_nrocomercio_fk;
				     alter table cliente drop constraint cliente_pk;
					 alter table tarjeta drop constraint tarjeta_pk;
					 alter table comercio drop constraint comercio_pk;
					 alter table compra drop constraint compra_pk;
					 alter table rechazo drop constraint rechazo_pk;
					 alter table cierre drop constraint cierre_pk;
					 alter table cabecera drop constraint cabecera_pk;
					 alter table detalle drop constraint detalle_pk;
					 alter table alerta drop constraint alerta_pk;
					 `)
	if err != nil {
		log.Fatal(err)
	}

}

func autorizacionCompra() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
					create or replace function autorizarCompra(nrotarjetaP char(16), codseguridadP char(4), montoP decimal(8,2), nrocomercioP int) returns boolean as $$
						declare

							estadotarjeta char(10);
							codigoencontrado char(4);
							deuda decimal(8,2);
							resultado record;
							limitecompra decimal(8,2);
							añovencimiento int;
							mesvencimiento int;
							añoactual int;
							mesactual int;

						begin
							
							select * into resultado from tarjeta where nrotarjeta = nrotarjetaP;

							if not found then

								--TARJETA INEXISTENTE (NO EXISTE EL NRO DE TARJETA).

								return false;

							else
								--TARJETA EXISTENTE (EXISTE EL NRO DE TARJETA).
								estadotarjeta := resultado.estado;
								codigoencontrado := resultado.codseguridad;
								limitecompra := resultado.limitecompra;

								añovencimiento := cast (substring(resultado.validahasta from 1 for 4) as integer);
								mesvencimiento := cast (substring(resultado.validahasta from 5 for 6) as integer);
								añoactual := cast (date_part('year', now()) as integer  );
								mesactual := cast (date_part('month', now()) as integer  );

								
								--TARJETA ANULADA
								if estadotarjeta = 'anulada' then
									insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'tarjeta no válida ó no vigente');
									return false;

								--TARJETA SUSPENDIDA
								elsif estadotarjeta = 'suspendida' then
									insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'la tarjeta se encuentra suspendida');
									return false;

								else
									--TARJETA VÁLIDA (ESTADO = VÁLIDO)
									--CÓDIGO DE SEGURIDAD INVÁLIDO
									if codigoencontrado != codseguridadP then
										insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'código de seguridad inválido');
										return false;

									--PLAZO DE VIGENCIA. TARJETA VENCIDA
									elsif añoactual>añovencimiento OR (añoactual=añovencimiento AND mesactual>mesvencimiento)  then
										insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'plazo de vigencia expirado');
										return false;

									--CÓDIGO DE SEGURIDAD VÁLIDO
									else 
										--EL MONTO SOLICIDATO SUPERA EL LIMITE
										if  montoP > limitecompra then
											insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'supera el límite de compra');
											return false;
										

										else 				
											select sum (monto) into deuda from compra where nrotarjeta = nrotarjetaP AND pagado=false;
											if found then
												deuda := deuda + montoP;
												if deuda > limitecompra then
													--LA SUMA DE LOS MONTOS QUE DEBE PAGAR MÁS EL ACTUAL, SUPERA EL LIMITE
													insert into rechazo values (default, nrotarjetaP, nrocomercioP, current_timestamp, montoP, 'supera el límite de compra');
													return false;

													
											

												--COMPRA VÁLIDA
												else
													return true;

												end if;


											end if;


										end if;

									end if;


								end if;
							
							end if;

					end;
					$$language plpgsql;		
					`)
	if err != nil {
		log.Fatal(err)
	}

}

func generacionResumen() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
					create or replace function generarResumen(nroclienteP int, desdeP date, hastaP date, fechavtoP date, nombrecliente text, apellidocliente text, domiciliocliente text, tarjetacliente char(16)) returns void as $$
					
						declare

							resultado record;
							
														
							deuda decimal(8,2);

							
							v record;
							nombrecomercio text;

							ultimoid int;
							
						begin

							select sum (monto) into deuda from compra where nrotarjeta = tarjetacliente AND pagado=false AND cast(fecha as date) between desdeP and hastaP ;
	
							insert into cabecera values (default, nombrecliente, apellidocliente, domiciliocliente, tarjetacliente, desdeP, hastaP, fechavtoP, deuda);

	
							select * into resultado from cabecera order by nroresumen desc limit 1;
							ultimoid := resultado.nroresumen;

							for v in select * from compra where pagado=false AND nrotarjeta = tarjetacliente AND cast(fecha as date) between desdeP and hastaP loop
										
										
								select * into resultado from comercio where nrocomercio = v.nrocomercio;
								nombrecomercio := resultado.nombre;
										 
								insert into detalle values (ultimoid, default, v.fecha, nombrecomercio, v.monto);
										
							end loop;

							
						
					end;
					$$language plpgsql;		
					`)
	if err != nil {
		log.Fatal(err)
	}

	agregarCierres()

	_, err = db.Exec(`
					create or replace function hacerResumen() returns void as $$
					
						declare
	
							v record;
							
														
							b record;

							resultado record;

							c record;

							terminaciontarjeta int;
							
							
						begin
 		
						for v in select * from cliente loop

													
								for b in select * from tarjeta where nrocliente=v.nrocliente  loop

									terminaciontarjeta := cast( substring(b.nrotarjeta from 16 for 16) as integer);

									for c in select *  from cierre where terminacion=terminaciontarjeta loop
										
										perform generarResumen(v.nrocliente,  c.fechainicio , c.fechacierre ,c.fechavto, v.nombre, v.apellido, v.domicilio, b.nrotarjeta);

									end loop;
									
								end loop;	
									
						end loop;	 
													
						
						
					end;
					$$language plpgsql;		
					`)
	if err != nil {
		log.Fatal(err)
	}

}

func agregarCierres() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	insert into cierre values (2022, 1, 0, '2022-01-01', '2022-01-31', '2022-02-08');
	insert into cierre values (2022, 1, 1, '2022-01-02', '2022-02-01', '2022-02-09');
	insert into	cierre values (2022, 1, 2, '2022-01-03', '2022-02-02', '2022-02-10');
	insert into	cierre values (2022, 1, 3, '2022-01-04', '2022-02-03', '2022-02-11');
	insert into	cierre values (2022, 1, 4, '2022-01-05', '2022-02-04', '2022-02-12');
	insert into	cierre values (2022, 1, 5, '2022-01-06', '2022-02-05', '2022-02-13');
	insert into	cierre values (2022, 1, 6, '2022-01-07', '2022-02-06', '2022-02-14');
	insert into	cierre values (2022, 1, 7, '2022-01-08', '2022-02-07', '2022-02-15');
	insert into	cierre values (2022, 1, 8, '2022-01-09', '2022-02-08', '2022-02-16');
	insert into	cierre values (2022, 1, 9, '2022-01-10', '2022-02-09', '2022-02-17');
	insert into	cierre values (2022, 2, 0, '2022-02-01', '2022-02-28', '2022-03-06');
	insert into	cierre values (2022, 2, 1, '2022-02-02', '2022-03-01', '2022-03-07');
	insert into	cierre values (2022, 2, 2, '2022-02-03', '2022-03-02', '2022-03-08');
	insert into	cierre values (2022, 2, 3, '2022-02-04', '2022-03-03', '2022-03-09');
	insert into	cierre values (2022, 2, 4, '2022-02-05', '2022-03-04', '2022-03-10');
	insert into	cierre values (2022, 2, 5, '2022-02-06', '2022-03-05', '2022-03-11');
	insert into	cierre values (2022, 2, 6, '2022-02-07', '2022-03-06', '2022-03-12');
	insert into	cierre values (2022, 2, 7, '2022-02-08', '2022-03-07', '2022-03-13');
	insert into	cierre values (2022, 2, 8, '2022-02-09', '2022-03-08', '2022-03-14');
	insert into	cierre values (2022, 2, 9, '2022-02-10', '2022-03-09', '2022-03-15');
	insert into cierre values (2022, 3,	0, '2022-03-01', '2022-03-31', '2022-04-07');
	insert into	cierre values (2022, 3,	1, '2022-03-02', '2022-04-01', '2022-04-08');
	insert into	cierre values (2022, 3,	2, '2022-03-03', '2022-04-02', '2022-04-09');
	insert into	cierre values (2022, 3,	3, '2022-03-04', '2022-04-03', '2022-04-10');
	insert into	cierre values (2022, 3,	4, '2022-03-05', '2022-04-04', '2022-04-11');
	insert into	cierre values (2022, 3,	5, '2022-03-06', '2022-04-05', '2022-04-12');
	insert into	cierre values (2022, 3,	6, '2022-03-07', '2022-04-06', '2022-04-13');
	insert into	cierre values (2022, 3,	7, '2022-03-08', '2022-04-07', '2022-04-14');
	insert into	cierre values (2022, 3,	8, '2022-03-09', '2022-04-08', '2022-04-15');
	insert into	cierre values (2022, 3,	9, '2022-03-10', '2022-04-09', '2022-04-16');
	insert into	cierre values (2022, 4,	0, '2022-04-01', '2022-04-30', '2022-05-05');
	insert into	cierre values (2022, 4,	1, '2022-04-02', '2022-05-01', '2022-05-06');
	insert into	cierre values (2022, 4,	2, '2022-04-03', '2022-05-02', '2022-05-07');
	insert into	cierre values (2022, 4,	3, '2022-04-04', '2022-05-03', '2022-05-08');
	insert into	cierre values (2022, 4,	4, '2022-04-05', '2022-05-04', '2022-05-09');
	insert into	cierre values (2022, 4,	5, '2022-04-06', '2022-05-05', '2022-05-10');
	insert into	cierre values (2022, 4,	6, '2022-04-07', '2022-05-06', '2022-05-11');
	insert into	cierre values (2022, 4,	7, '2022-04-08', '2022-05-07', '2022-05-12');
	insert into	cierre values (2022, 4,	8, '2022-04-09', '2022-05-08', '2022-05-13');
	insert into	cierre values (2022, 4,	9, '2022-04-10', '2022-05-09', '2022-05-14');
	insert into	cierre values (2022, 5,	0, '2022-05-01', '2022-05-31', '2022-06-06');
	insert into	cierre values (2022, 5,	1, '2022-05-02', '2022-06-01', '2022-06-07');
	insert into	cierre values (2022, 5,	2, '2022-05-03', '2022-06-02', '2022-06-08');
	insert into	cierre values (2022, 5,	3, '2022-05-04', '2022-06-03', '2022-06-09');
	insert into	cierre values (2022, 5,	4, '2022-05-05', '2022-06-04', '2022-06-10');
	insert into	cierre values (2022, 5,	5, '2022-05-06', '2022-06-05', '2022-06-11');
	insert into	cierre values (2022, 5,	6, '2022-05-07', '2022-06-06', '2022-06-12');
	insert into	cierre values (2022, 5,	7, '2022-05-08', '2022-06-07', '2022-06-13');
	insert into	cierre values (2022, 5,	8, '2022-05-09', '2022-06-08', '2022-06-14');
	insert into	cierre values (2022, 5,	9, '2022-05-10', '2022-06-09', '2022-06-15');
	insert into	cierre values (2022, 6,	0, '2022-06-01', '2022-06-30', '2022-07-08');
	insert into	cierre values (2022, 6,	1, '2022-06-02', '2022-07-01', '2022-07-09');
	insert into	cierre values (2022, 6,	2, '2022-06-03', '2022-07-02', '2022-07-10');
	insert into	cierre values (2022, 6,	3, '2022-06-04', '2022-07-03', '2022-07-11');
	insert into	cierre values (2022, 6,	4, '2022-06-05', '2022-07-04', '2022-07-12');
	insert into	cierre values (2022, 6,	5, '2022-06-06', '2022-07-05', '2022-07-13');
	insert into	cierre values (2022, 6,	6, '2022-06-07', '2022-07-06', '2022-07-14');
	insert into	cierre values (2022, 6,	7, '2022-06-08', '2022-07-07', '2022-07-15');
	insert into	cierre values (2022, 6,	8, '2022-06-09', '2022-07-08', '2022-07-16');
	insert into	cierre values (2022, 6,	9, '2022-06-10', '2022-07-09', '2022-07-17');
	insert into	cierre values (2022, 7,	0, '2022-07-01', '2022-07-31', '2022-08-04');
	insert into	cierre values (2022, 7,	1, '2022-07-02', '2022-08-01', '2022-08-05');
	insert into	cierre values (2022, 7,	2, '2022-07-03', '2022-08-02', '2022-08-06');
	insert into	cierre values (2022, 7,	3, '2022-07-04', '2022-08-03', '2022-08-07');
	insert into	cierre values (2022, 7,	4, '2022-07-05', '2022-08-04', '2022-08-08');
	insert into	cierre values (2022, 7,	5, '2022-07-06', '2022-08-05', '2022-08-09');
	insert into	cierre values (2022, 7,	6, '2022-07-07', '2022-08-06', '2022-08-10');
	insert into	cierre values (2022, 7,	7, '2022-07-08', '2022-08-07', '2022-08-11');
	insert into	cierre values (2022, 7,	8, '2022-07-09', '2022-08-08', '2022-08-12');
	insert into	cierre values (2022, 7,	9, '2022-07-10', '2022-08-09', '2022-08-13');
	insert into	cierre values (2022, 8,	0, '2022-08-01', '2022-08-21', '2022-09-05');
	insert into	cierre values (2022, 8,	1, '2022-08-02', '2022-08-22', '2022-09-06');
	insert into	cierre values (2022, 8,	2, '2022-08-03', '2022-08-23', '2022-09-07');
	insert into	cierre values (2022, 8,	3, '2022-08-04', '2022-08-24', '2022-09-08');
	insert into	cierre values (2022, 8,	4, '2022-08-05', '2022-08-25', '2022-09-09');
	insert into	cierre values (2022, 8,	5, '2022-08-06', '2022-08-26', '2022-09-10');
	insert into	cierre values (2022, 8,	6, '2022-08-07', '2022-08-27', '2022-09-11');
	insert into	cierre values (2022, 8,	7, '2022-08-08', '2022-08-28', '2022-09-12');
	insert into	cierre values (2022, 8,	8, '2022-08-09', '2022-08-29', '2022-09-13');
	insert into	cierre values (2022, 8,	9, '2022-08-10', '2022-08-30', '2022-09-14');
	insert into	cierre values (2022, 9,	0, '2022-09-01', '2022-09-30', '2022-10-06');
	insert into	cierre values (2022, 9,	1, '2022-09-02', '2022-10-01', '2022-10-07');
	insert into	cierre values (2022, 9,	2, '2022-09-03', '2022-10-02', '2022-10-08');
	insert into	cierre values (2022, 9,	3, '2022-09-04', '2022-10-03', '2022-10-09');
	insert into	cierre values (2022, 9,	4, '2022-09-05', '2022-10-04', '2022-10-10');
	insert into	cierre values (2022, 9,	5, '2022-09-06', '2022-10-05', '2022-10-11');
	insert into	cierre values (2022, 9,	6, '2022-09-07', '2022-10-06', '2022-10-12');
	insert into	cierre values (2022, 9,	7, '2022-09-08', '2022-10-07', '2022-10-13');
	insert into	cierre values (2022, 9,	8, '2022-09-09', '2022-10-08', '2022-10-14');
	insert into	cierre values (2022, 9,	9, '2022-09-10', '2022-10-09', '2022-10-15');
	insert into	cierre values (2022, 10, 0, '2022-10-01', '2022-10-31', '2022-11-04');
	insert into	cierre values (2022, 10, 1,	'2022-10-02', '2022-11-01', '2022-11-05');
	insert into	cierre values (2022, 10, 2,	'2022-10-03', '2022-11-02', '2022-11-06');
	insert into	cierre values (2022, 10, 3,	'2022-10-04', '2022-11-03', '2022-11-07');
	insert into	cierre values (2022, 10, 4,	'2022-10-05', '2022-11-04',	'2022-11-08');
	insert into	cierre values (2022, 10, 5,	'2022-10-06', '2022-11-05',	'2022-11-09');
	insert into	cierre values (2022, 10, 6,	'2022-10-07', '2022-11-06',	'2022-11-10');
	insert into	cierre values (2022, 10, 7,	'2022-10-08', '2022-11-07',	'2022-11-11');
	insert into	cierre values (2022, 10, 8,	'2022-10-09', '2022-11-08',	'2022-11-12');
	insert into	cierre values (2022, 10, 9,	'2022-10-10', '2022-11-09',	'2022-11-13');
	insert into	cierre values (2022, 11, 0,	'2022-11-01', '2022-11-30',	'2022-12-05');
	insert into	cierre values (2022, 11, 1,	'2022-11-02', '2022-12-01',	'2022-12-06');
	insert into	cierre values (2022, 11, 2,	'2022-11-03', '2022-12-02',	'2022-12-07');
	insert into	cierre values (2022, 11, 3,	'2022-11-04', '2022-12-03',	'2022-12-08');
	insert into	cierre values (2022, 11, 4,	'2022-11-05', '2022-12-04',	'2022-12-09');
	insert into	cierre values (2022, 11, 5,	'2022-11-06', '2022-12-05',	'2022-12-10');
	insert into	cierre values (2022, 11, 6,	'2022-11-07', '2022-12-06',	'2022-12-11');
	insert into	cierre values (2022, 11, 7,	'2022-11-08', '2022-12-07',	'2022-12-12');
	insert into	cierre values (2022, 11, 8,	'2022-11-09', '2022-12-08',	'2022-12-13');
	insert into	cierre values (2022, 11, 9,	'2022-11-10', '2022-12-09',	'2022-12-14');
	insert into	cierre values (2022, 12, 0,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 1,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 2,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 3,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 4,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 5,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 6,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 7,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 8,	'2022-12-01', '2022-12-31',	'2023-01-03');
	insert into	cierre values (2022, 12, 9,	'2022-12-01', '2022-12-31',	'2023-01-03');
`)
	if err != nil {
		log.Fatal(err)
	}
}

func compra() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
					create or replace function hacerCompra() returns void as $$
					
						declare
						
							v record;
							b record;
							
							nrotarjetaconsumo char(16);
							codseguridadconsumo char(4);
							montoconsumo decimal(7,2);
							nrocomercioconsumo int;
							
						begin

							for v in select * from consumo loop

								nrotarjetaconsumo := v.nrotarjeta;
								codseguridadconsumo := v.codseguridad;
								montoconsumo := v.monto;
								nrocomercioconsumo := v.nrocomercio;

								if autorizarCompra(nrotarjetaconsumo, codseguridadconsumo, montoconsumo, nrocomercioconsumo) then
									insert into compra values( default, nrotarjetaconsumo, nrocomercioconsumo, current_timestamp, montoconsumo, false);
								end if;
								
																
							end loop;
						

							
						
					end;
					$$language plpgsql;		
					`)
	if err != nil {
		log.Fatal(err)
	}
}

func alertaClientes() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
					create or replace function alertasAClientes() returns trigger as $$
					
						declare
							resultado record;
															
							
							año int;
							mes int;
							dia int;
							hora int;
							minuto int;
							nrocomercioultima int;
							codpostalultima char(8);
								

							
							añoIngreso int;
							mesIngreso int;
							diaIngreso int;
							horaIngreso int;
							minutoIngreso int;
							codigopostalIngreso char(8);
							nrocomercioIngreso int;
									
							ultimoid int;

						
						
							

						begin

							select *  into resultado from compra where nrotarjeta=new.nrotarjeta order by nrooperacion desc limit 1;
							if found then 
														 
								
								año := cast(extract (year from resultado.fecha) as int);	
								mes := cast(extract (month from resultado.fecha) as int);
								dia := cast(extract (day from resultado.fecha) as int);
								hora := cast(extract (hour from resultado.fecha) as int);
								minuto := cast(extract (minute from resultado.fecha) as int);
								nrocomercioultima := resultado.nrocomercio;

								select * into resultado from comercio where nrocomercio = nrocomercioultima;

								codpostalultima := resultado.codigopostal; 

								
								añoIngreso := cast(extract (year from new.fecha) as int);	
								mesIngreso := cast(extract (month from new.fecha) as int);
								diaIngreso := cast(extract (day from new.fecha) as int);
								horaIngreso := cast(extract (hour from new.fecha) as int);
								minutoIngreso := cast(extract (minute from new.fecha) as int);								
								nrocomercioIngreso :=  new.nrocomercio;

							
								

								select * into resultado from comercio where nrocomercio = new.nrocomercio;
								codigopostalIngreso := resultado.codigopostal;
								
								--COMPRA EN MENOS DE 1 MINUTO EN DISTINTOS COMERCIOS CON EL MISMO CODIGO POSTAL
								if nrocomercioultima!=nrocomercioIngreso AND añoIngreso=año AND mesIngreso=mes AND diaIngreso=dia AND horaIngreso=hora AND minutoIngreso-minuto<=1 AND codpostalultima=codigopostalIngreso then
																				
								
																						
									insert into alerta values (default, new.nrotarjeta, current_date, null, 1,'compra en menos de 1 minutos en comercios con  distinto codigo postal');			

								--COMPRA EN MENOS DE 5 MINUTOS EN COMERCIOS CON DISTINTO CODIGO POSTAL
								elsif  añoIngreso=año AND mesIngreso=mes AND diaIngreso=dia AND horaIngreso=hora AND minutoIngreso-minuto<=5 AND codpostalultima!=codigopostalIngreso then
																														
									insert into alerta values (default, new.nrotarjeta, current_date, null, 5,'compra en menos de 5 minuto en comercios con distinto código postal');

												
								end if;

							end if;
							return new;
							
							
					end;
					$$language plpgsql;		

					create trigger alertasAClientes_trg
					before insert on compra
					for each row
					execute procedure alertasAClientes();`)
	if err != nil {
		log.Fatal(err)
	}

}

func creacionTriggers() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	_, err = db.Exec(`
					create or replace function count_function(tarjeta char(16)) returns integer as $$
					
													
						begin
							return (select count(*) from rechazo where nrotarjeta = tarjeta AND motivo='supera el límite de compra' AND current_date=cast(fecha as date));
												
					end;
					$$language plpgsql;	`)
	if err != nil {
		log.Fatal(err)
	}


	//=========TRIGER QUE INSERTA UNA ALERTA DESPUES DE HABER INSERTADO UN REGISTRO EL RECHAZO====================
	_, err = db.Exec(`
						create or replace function creacionAlerta() returns trigger as $$
						
							declare
							
								resultado record;
								
														
							begin

								--Chequea si tiene dos rechazos por exceso de limite												
								if count_function(new.nrotarjeta)>=2 then
								
									select * into resultado from rechazo order by nrorechazo desc limit 1;
									UPDATE tarjeta set estado='suspendida' WHERE nrotarjeta = new.nrotarjeta;
									insert into alerta values(default, new.nrotarjeta, current_timestamp, resultado.nrorechazo, 32, 'La tarjeta registra dos rechazos por exceso de límite en el mismo dìa');
							
								elsif new.motivo = 'supera el límite de compra' then 
									insert into alerta values (default, new.nrotarjeta, new.fecha, new.nrorechazo, 32,'supera limite de compra');
								elsif new.motivo = 'la tarjeta se encuentra suspendida' then
									insert into alerta values (default, new.nrotarjeta, new.fecha, new.nrorechazo, 0, 'la tarjeta se encuentra suspendida');
								elsif new.motivo = 'código de seguridad inválido' then
									insert into alerta values (default, new.nrotarjeta, new.fecha, new.nrorechazo, 0, 'la tarjeta se encuentra suspendida');
								elsif new.motivo = 'plazo de vigencia expirado' then
										insert into alerta values (default, new.nrotarjeta, new.fecha, new.nrorechazo, 0, 'plazo de vigencia expirado');
								elsif new.motivo = 'tarjeta no válida ó no vigente' then
										insert into alerta values (default, new.nrotarjeta, new.fecha, new.nrorechazo, 0, 'tarjeta no valida o no vigente');	
								end if;
												
								return new;
								
								
						end;
						$$language plpgsql;		
	
						create trigger creacionAlerta_trg
						after insert on rechazo
						for each row
						execute procedure creacionAlerta();`)
	if err != nil {
		log.Fatal(err)
	}
}

func menu() {

	var opciones = `*****************************************
|                Ingrese                    |
|-1: salir                                  |
| 0: crear base de datos, tablas y funciones|
| 1: agregar claves                         |
| 2: agregar los registros                  |
| 3: elminar las claves                     |
| 4: pasar consumos a compras          	    |
| 5: generar resumen de tarjetas            |
********************************************`

	var opcion int
	salir := false

	fmt.Println(opciones)

	for !salir {

		fmt.Scanf("%d", &opcion)

		switch opcion {
		case -1:
			salir = true
			
		case 0:
			createDatabase()
			createTables()
			autorizacionCompra()
			generacionResumen()
			alertaClientes()
			creacionTriggers()

			fmt.Printf("Base de datos y funciones creadas.\n\n")
			break

		case 1:
			agregarClaves()
			fmt.Printf("Claves (PK's y FK's) agregadas.\n\n")
			break
		case 2:
			agregarRegistros()
			fmt.Printf("Registros agregados.\n\n")
			break
		case 3:
			dropKeys()
			fmt.Printf("Claves (PK's y FK's) eliminadas.\n\n")
			break
		case 4:
			compra()
			db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
			if err != nil {
				log.Fatal(err)
			}
			defer db.Close()

			_, err = db.Exec(`select hacerCompra();`)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Compras actualizadas.\n\n")

			break
		case 5:

			db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
			if err != nil {
				log.Fatal(err)
			}
			defer db.Close()

			_, err = db.Exec(`select hacerResumen();`)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Resúmenes realizados.\n\n")
			break
		default:
			fmt.Printf("La opción %v es INVÁLIDA.\n\n", opcion)
		}
	}

}

func main() {

	menu()
}
