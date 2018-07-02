from elasticsearch import Elasticsearch
import random
import csv
import time

folio = [None, 100, 200, 300, 400]

folioMovilidad = [None, 1]

etapa_cliente = ["Prospecto", "Solicitud", "Contratado"]

estatus_cliente = ["Sin Folio", "Proceso", "Aprobada", "Rechazada", "Autorizada", "Agendado", "Firmado", "Pendiente",
                   "Rechazado", "Solventada"]

estatus_seguimiento_cartera = ["en espera", "terminado", "no terminado"]

# ALB: Se cambia el estatus del cliente para ue la distribución de un 50% de prospectos con flag_prosṕector == true
sap_estatus_cliente = [37, 38, 60, 61]

# ALB: El estatus del cliente siempre debe de ser nulo
sap_subestatus_cliente = [40, 41, 42, 43, 44, 45]

# ALB: La subetapa del cliente siempre debe de ser null
subetapa_cliente = ["Sin Gestión", "Pre Solicitud", "Análisis", "Autorización", "Agenda", "Firma"]

macroproceso_cliente = ["Prospección", "Análisis", "Autorización", "Instrumentación"]

pantalla_cliente = ["Pre Gestión", "Ofertas", "Validar información", "Validar documentación", "Autorización", "Agendar",
                    "Consultar agenda"]

# ALB: Se cambia el campo de autoriza_oferta a que siempre sea false
autoriza_oferta = ["true", "false"]

flag_prospecto = ["true", "false"]

riesgos_producto = ["Crédito Simple S/G", "Renovación"]

riesgos_subProducto = ["OMEGA", "OMEGA_M", "OMEGA_S", "OMEGA_T", "UNID.FAMIL", "RENOVACION", "TOP UP"]

flag_ruta = [1, 2, 3, 4]

promotores = []
nombres = []
apellidos = []

file_bulk_renovaciones = open("elastic_renovaciones.json", "w")

file_bulk_trazabilidad = open("elastic_trazabilidad.json", "w")


class Promotor:
    nombre = ''
    apellido = ''
    id = 0
    gerenciaVentas = ''


def openPromotor():
    with open("promotor.csv", newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            promotor = Promotor()
            promotor.nombre = row[0]
            promotor.apellido = row[1]
            promotor.id = row[2]
            promotor.gerenciaVentas = row[3]
            promotores.append(promotor)


def openNombres():
    with open("nombres.csv", newline='') as File:
        reader = csv.reader(File)
        for row in reader:
            nombres.append(row[0])


def openApellidos():
    with open("apellidos.csv", newline='') as File:
        reader = csv.reader(File)
        for row in reader:
            apellidos.append(row[0])


# "2018-05-03T13:42:20.760"
def strTimeProp(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


# '%m/%d/%Y %I:%M %p'

def randomDate(start, end, prop):
    return strTimeProp(start, end, '%Y-%m-%dT%H:%M:%S', prop)


def build_doc_renovacionestopup(idCliente, id, value_null, Sap_estatus_cliente, Sap_subestatus_cliente,
                                Estatus_seguimientoCartera, Folio, Etapa_cliente, Estatus_cliente, Subetapa_cliente,
                                Folio_Movilidad, Pantalla_cliente, no_contrato, Macroproceso_cliente,
                                Flag_prospecto, promotorIt, nombreSAP1, nombreSAP2,
                                apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion,
                                Riesgos_producto, Riesgos_subProducto,
                                Flag_ruta):  # valueNull, value
    doc = {
        "CRM_calle_cliente": "Priv 18",
        "CRM_ciudad": "Mexico",
        "CRM_codigoPostal_domicilioCliente": "54678",
        "CRM_colonia_domicilioCliente": "Huehue",
        "CRM_descripcionContacto_telefono_1": "Casa",
        "CRM_descripcionContacto_telefono_2": "Casa2",
        "CRM_descripcionContacto_telefono_3": "Casa3",
        "CRM_descripcionContacto_telefono_4": "Casa4",
        "CRM_descripcionContacto_telefono_5": "Casa5",
        "CRM_entreCalle_1": "Calle1",
        "CRM_entreCalle_2": "calle2",
        "CRM_estado": "Mexico",
        "CRM_extension_telefono_1": "5567678978",
        "CRM_extension_telefono_2": "5567456978",
        "CRM_extension_telefono_3": "5512980381",
        "CRM_extension_telefono_4": "5512983393",
        "CRM_extension_telefono_5": "5591028391",
        "CRM_fecha_contacto_telefono_1": "2018-05-03T13:42:20.760",
        "CRM_fecha_contacto_telefono_2": "2018-04-03T13:42:20.760",
        "CRM_fecha_contacto_telefono_3": "2018-03-03T13:42:20.760",
        "CRM_fecha_contacto_telefono_4": "2018-02-03T13:42:20.760",
        "CRM_fecha_contacto_telefono_5": "2018-01-03T13:42:20.760",
        "CRM_intentos_telefono_1": 1,
        "CRM_intentos_telefono_2": 2,
        "CRM_intentos_telefono_3": 3,
        "CRM_intentos_telefono_4": 4,
        "CRM_intentos_telefono_5": 5,
        "CRM_lada_telefono_1": "234",
        "CRM_lada_telefono_2": "234",
        "CRM_lada_telefono_3": "234",
        "CRM_lada_telefono_4": "234",
        "CRM_lada_telefono_5": "234",
        "CRM_lote_domicilioCliente": "Lt4",
        "CRM_manzana_domicilioCliente": "64",
        "CRM_municipio_domicilioCliente": "Naui",
        "CRM_numeroInterior_domicilioCliente": "3",
        "CRM_numero_domicilioCliente": "12893123",
        "CRM_pais": "Mexico",
        "CRM_telefono_1": "551291231",
        "CRM_telefono_2": "551291242",
        "CRM_telefono_3": "551291253",
        "CRM_telefono_4": "551291264",
        "CRM_telefono_5": "551291275",
        "CRM_tipoContacto_telefono_1": "tipoContacto",
        "CRM_tipoContacto_telefono_2": "tipoContacto",
        "CRM_tipoContacto_telefono_3": "tipoContacto",
        "CRM_tipoContacto_telefono_4": "tipoContacto",
        "CRM_tipoContacto_telefono_5": "tipoContacto",
        "CRM_tipoDomicilio_cliente": "TipoDomi",
        "CRM_tipoTelefono_1": "TipoTelefono",
        "CRM_tipoTelefono_2": "TipoTelefono",
        "CRM_tipoTelefono_3": "TipoTelefono",
        "CRM_tipoTelefono_4": "TipoTelefono",
        "CRM_tipoTelefono_5": "TipoTelefono",
        "CRM_tipoVivienda_cliente": "TipoVivienda1",
        "ITA_monto": 123.40,
        "ITA_numeroPagos_1": 1,
        "ITA_numeroPagos_2": 2,
        "ITA_numeroPagos_3": 3,
        "ITA_numeroPagos_4": 4,
        "SAP_apellidoMaterno_cliente": apellidoSAPMater,
        "SAP_apellidoPaterno_cliente": apellidoSAPPater,
        "SAP_calleDomicilio_1": "calleDoomi",
        "SAP_calleDomicilio_2": "calleDomi",
        "SAP_calleDomicilio_cliente": "CalleDomi",
        "SAP_clave_coloniaDomicilio": "ClaveColonia",
        "SAP_clave_municipioDomicilio": "ClaveDoni",
        "SAP_codigoPostalDomicilio": "55908",
        "SAP_coloniaDomicilio": "coloniaDomi",
        "SAP_descripcionContacto_telefono_1": "SAPCasa",
        "SAP_descripcionContacto_telefono_2": "SAPCasa",
        "SAP_estadoDomicilio": "Mexico,Huai",
        "SAP_estatus_cliente": Sap_estatus_cliente,
        "SAP_fecha_contacto_telefono_1": "2018-05-03T13:42:20.760",
        "SAP_fecha_contacto_telefono_2": "2018-05-03T13:42:20.760",
        "SAP_geolocalizacion_x_domicilio": "XDomi",
        "SAP_geolocalizacion_y_domicilio": "YDomi",
        "SAP_intentos_telefono_1": 1,
        "SAP_intentos_telefono_2": 2,
        "SAP_municipioDomicilio": "MuniDomi",
        "SAP_nombre_cliente_1": nombreSAP1,
        "SAP_nombre_cliente_2": nombreSAP2,
        "SAP_numeroDomicilio_cliente": "12",
        "SAP_producto": "Producto",
        "SAP_subProducto": "Renovacion",
        "SAP_subestatus_cliente": Sap_subestatus_cliente,
        "SAP_telefono_1": phn(),
        "SAP_telefono_2": phn(),
        "SAP_tipoContacto_telefono_1": "tipoContacto",
        "SAP_tipoContacto_telefono_2": "tipoContacto",
        "TP_telefono_tipificacacion": "55891283312",
        "TP_telefono_tipificacion_etiqueta": "1231234123",
        "TP_tipo_tipificacion_telefono": "tipoTipificacion",
        "contrato": 123,
        "curp_cliente": "AARM981209HMCMK09",
        "detalle_oferta": "Renovacion",
        "estatus_campania": "1",
        "estatus_carga": "1",
        # ALB: El estatus del seguimiento de la cartera siempre debe de ser nulo
        "estatus_seguimientoCartera": None,
        "etiqueta_contacto_telefonos_actualizados": "contactado",
        "etiqueta_oferta": "etiqueta oferta",
        "etiqueta_telefonos_actualizados": "Etiqueta2",
        "fecha_activacion_campania": "2018-05-03T13:42:20.760",
        "fecha_cargaArchivo_layoutOfertas": "2018-05-03T13:42:20.760",
        "fecha_insert": "2018-05-03T13:42:20.760",
        "fecha_prospeccion": fechaProspeccion,
        "flag_ruta": Flag_ruta,
        "generadoUsuario_descripcionContacto_telefono": "contacto",
        "generadoUsuario_extension_telefono": "2212",
        "generadoUsuario_fecha_contacto_telefono": "2018-05-03T13:42:20.760",
        "generadoUsuario_intentosContacto_telefono": 4,
        "generadoUsuario_telefono": "5512982391",
        "generadoUsuario_tipo_telefono": "tipoGenerado",
        # ALB: Se agrega la gerencia de Ventas
        "gerenciadeVentas": promotorIt.gerenciaVentas,
        "idCliente": idCliente,
        "idProspeccion": id,
        "id_campania": id,
        "ingreso": 1200.90,
        "localidad_cliente": "Mexico",
        "montoNuevaOfertaAutomatica": 100,
        "montoNuevaOfertaAutomaticaMaxima": value_null,
        "montoOfertaAutomaticaMaxima_1": 200,
        "montoOfertaAutomaticaMaxima_2": 300,
        "montoOfertaAutomaticaMaxima_3": 400,
        "montoOfertaAutomaticaMaxima_4": 500,
        "montoOfertaAutomatica_1": 600,
        "montoOfertaAutomatica_2": 700,
        "montoOfertaAutomatica_3": 800,
        "montoOfertaAutomatica_4": 900,
        "montoOfertaRequeridoProspecto": 100,
        "monto_producto_actual": 20000.00,
        "motivo_oferta": "NA",
        "nivel_riesgo_1": "Bajo",
        "nivel_riesgo_actual": "Bajo",
        "nombre_lista_marketing": "12082018_TCCP",
        "numeroPagosNuevaOfertaAutomatica": 100,
        "numeroPagosNuevaOfertaAutomaticaMaxima": 200,
        "numeroPagosOfertaAutomaticaMaxima_1": 300,
        "numeroPagosOfertaAutomaticaMaxima_2": 400,
        "numeroPagosOfertaAutomaticaMaxima_3": 500,
        "numeroPagosOfertaAutomaticaMaxima_4": 600,
        "numeroPagosOfertaAutomatica_1": 700,
        "numeroPagosOfertaAutomatica_2": 800,
        "numeroPagosOfertaAutomatica_3": 900,
        "numeroPagosOfertaAutomatica_4": 100,
        "numeroPagosOfertaRequeridoProspecto": 2,
        "numeroPagos_producto_actual": 12,
        "ofertaSeleccionada": "StringPropiedadesOfertaSeleccionada",
        "pagoNuevaOfertaAutomatica": 100,
        "pagoNuevaOfertaAutomaticaMaxima": 200,
        "pagoOfertaAutomaticaMaxima_1": 300,
        "pagoOfertaAutomaticaMaxima_2": 400,
        "pagoOfertaAutomaticaMaxima_3": 500,
        "pagoOfertaAutomaticaMaxima_4": 600,
        "pagoOfertaAutomatica_1": 700,
        "pagoOfertaAutomatica_2": 800,
        "pagoOfertaAutomatica_3": 900,
        "pagoOfertaAutomatica_4": 100,
        "pagoOfertaRequeridoProspecto": 1290.00,
        "pago_producto_actual": 9000.00,
        "promotorApellidos": promotorIt.apellido,
        "promotorNombre": promotorIt.nombre,
        "rfc_cliente": "EABM091218S06",
        "riesgos_producto": Riesgos_producto,
        "riesgos_subProducto": Riesgos_subProducto,
        "seguimientoCartera": "cartera",
        "subProductoOferta": "Renovacion",
        "tasaNuevaOfertaAutomatica": 100,
        "tasaNuevaOfertaAutomaticaMaxima": 200,
        "tasaOfertaAutomaticaMaxima_1": 300,
        "tasaOfertaAutomaticaMaxima_2": 400,
        "tasaOfertaAutomaticaMaxima_3": 500,
        "tasaOfertaAutomaticaMaxima_4": 600,
        "tasaOfertaAutomatica_1": 700,
        "tasaOfertaAutomatica_2": 800,
        "tasaOfertaAutomatica_3": 20,
        "tasaOfertaAutomatica_4": 34,
        "tasaOfertaRequeridoProspecto": 12.00,
        "tasa_producto_actual": 13.00,
        "telefonos_actualizados": "5512901293@5590982378@551232467",
        "tipo_telefono_actualizados": "casa@oficina@departamento",
        "duracion_campania": 5,
        "folio_BPM": Folio,
        "etapa_cliente": Etapa_cliente,
        # ALB: El estatus del cliente siempre debe de estar en null
        "estatus_cliente": None,
        "ITA_tasa_1": 4,
        "ITA_tasa_2": 2,
        "ITA_tasa_3": 2,
        "ITA_tasa_4": 34.5,
        "fecha_nacimiento_cliente": "2018-09-03T13:42:20.760",
        # ALB: Se actualiza la fecha de la última gestion
        "fecha_ultima_gestion_cliente": fechaUltimaGestion,
        "folio_movilidad": Folio_Movilidad,
        # ALB: La subetapa del cliente siempre debe de ser nula
        "subetapa_cliente": None,
        # ALB: Autoriza oferta siempre nace como false
        "autoriza_oferta": "false",
        # ALB: La pantalla del cliente siempre debe de ser nula
        "pantalla_cliente": None,
        "numero_solicitud": None,
        "numeroContrato": no_contrato,
        # ALB: El macroproceso del cliente siempre debe de ser nulo
        "macroproceso_cliente": None,
        "flag_prospecto": Flag_prospecto,
        # ALB: Se agrega el ID del promotor
        "idPromotor": promotorIt.id,
        "fecha_desembolso_contrato": None,
        "sucursal_contrato": None,
        "folio_oferta": None,
        "tipo_pago_contrato": None,
        "fecha_numero_solicitud": None,
        "SAP_plazo": None,
        "SAP_monto": None,
        "SAP_numeroPagos": None,
        "SAP_tasa": None,
        "sociedad_contrato": None,
        "frecuencia_contrato": None,
        "partner_contrato": None,
        "origen_contrato": None,
        "promotor_contrato": None,
        "destino_contrato": None,
        "producto_contrato": None,
        "subproducto_contrato": None
    }
    return doc


def build_doc_trazabilidad_cliente(idCliente, id, SAP_estatus_cliente, SAP_subestatus_cliente,
                                   Estatus_seguimientoCartera, Folio, Etapa_cliente, Estatus_cliente,
                                   Subetapa_cliente, Pantalla_cliente, Macroproceso_cliente,
                                   no_contrato, Folio_Movilidad, Flag_prospecto):  # valueNull, value
    doc = {
        "etapa_cliente": Etapa_cliente,
        # ALB: El estatus del cliente siempre debe de ser nula
        "estatus_cliente": None,
        # ALB: La subetapa del cliente siempre debe de ser nula
        "subetapa_cliente": None,
        # ALB: El macroprocese del cliente siempre debe de ser null
        "macroproceso_cliente": None,
        # ALB: La pantalla del cliente siempre debe de ser null
        "pantalla_cliente": None,
        "folio_BPM": Folio,
        "folio_movilidad": Folio_Movilidad,
        "numero_solicitud": 1,
        "numeroContrato": no_contrato,
        "idCliente": idCliente,  # id_cliente
        "idProspeccion": id,  # id_prospeccion
        "id_campania": id,
        # ALB: La fecha de la última gestión del cliente debe de ser null
        "fecha_ultima_gestion_cliente": None,  # fecha_cambio_estatus
        "seguimientoCartera": "SeguimientoCartera",
        # ALB: El estatus de seguimiento de la cartera siempre debe de ser null
        "estatus_seguimientoCartera": None,
        "SAP_estatus_cliente": SAP_estatus_cliente,
        "SAP_subestatus_cliente": SAP_subestatus_cliente,
        "flag_prospecto": Flag_prospecto
    }
    return doc


def phn():
    p = list('0000000000')
    p[0] = str(random.randint(1, 9))
    for i in [1, 2, 6, 7, 8]:
        p[i] = str(random.randint(0, 9))
    for i in [3, 4]:
        p[i] = str(random.randint(0, 8))
    if p[3] == p[4] == 0:
        p[5] = str(random.randint(1, 8))
    else:
        p[5] = str(random.randint(0, 8))
    p[9] = str(random.randint(0, 9))
    p = ''.join(p)
    return p


def elastic_ingest_docs():
    id = 1  # hacer refenrencia al id campania
    openPromotor()
    openApellidos()
    openNombres()
    value_null = None

    for id_cliente in range(1, 1001):
        Flag_ruta = flag_ruta[random.randint(0, 3)]
        SAP_estatus_cliente = sap_estatus_cliente[random.randint(0, len(sap_estatus_cliente) - 1)]
        promotorIt = promotores[random.randint(0, len(promotores) - 1)]
        nombreSAP1 = nombres[random.randint(0, len(nombres) - 1)]
        apellidoSAPPater = apellidos[random.randint(0, len(apellidos) - 1)]
        apellidoSAPMater = apellidos[random.randint(0, len(apellidos) - 1)]
        fechaProspeccion = randomDate("2018-03-03T13:42:20", "2018-06-03T13:42:20", random.random())
        fechaUltimaGestion = randomDate(fechaProspeccion, "2018-06-03T13:42:20", random.random())
        nombreSAP2 = None
        if random.random() > 0.8:
            nombreSAP2 = nombres[random.randint(0, len(nombres) - 1)]
        Flag_prospecto = "false"
        if SAP_estatus_cliente == 60 or SAP_estatus_cliente == 61:
            Flag_prospecto = "true"

        if id_cliente <= 200:
            SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 5)]
            Estatus_seguimientoCartera = estatus_seguimiento_cartera[0]
            Folio_BPM = folio[0]
            Etapa_cliente = etapa_cliente[0]
            Estatus_cliente = estatus_cliente[0]
            Subetapa_cliente = subetapa_cliente[0]
            Pantalla_cliente = pantalla_cliente[0]
            Macroproceso_cliente = macroproceso_cliente[0]
            Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
            Riesgos_producto = riesgos_producto[0]
            Riesgos_subProducto = riesgos_subProducto[random.randint(0, 4)]

            bulk_indices(id_cliente, id, value_null, SAP_estatus_cliente,
                         SAP_subestatus_cliente, Estatus_seguimientoCartera, Folio_BPM,
                         Etapa_cliente,
                         Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, 1, Macroproceso_cliente,
                         Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente, promotorIt, nombreSAP1,
                         nombreSAP2,
                         apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion, Riesgos_producto,
                         Riesgos_subProducto, Flag_ruta)

        elif id_cliente <= 400 and id_cliente >= 201:
            SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 5)]
            Estatus_seguimientoCartera = estatus_seguimiento_cartera[0]
            Folio_BPM = folio[1]
            Etapa_cliente = etapa_cliente[1]
            Estatus_cliente = estatus_cliente[3]
            Subetapa_cliente = subetapa_cliente[2]
            Pantalla_cliente = pantalla_cliente[3]
            Macroproceso_cliente = macroproceso_cliente[1]
            Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
            Riesgos_producto = riesgos_producto[0]
            Riesgos_subProducto = riesgos_subProducto[random.randint(0, 4)]

            bulk_indices(id_cliente, id, value_null, SAP_estatus_cliente,
                         SAP_subestatus_cliente, Estatus_seguimientoCartera, Folio_BPM,
                         Etapa_cliente,
                         Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, 1, Macroproceso_cliente,
                         Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente, promotorIt, nombreSAP1,
                         nombreSAP2,
                         apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion, Riesgos_producto,
                         Riesgos_subProducto, Flag_ruta)

        elif id_cliente <= 600 and id_cliente >= 401:
            SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 5)]
            Estatus_seguimientoCartera = estatus_seguimiento_cartera[0]
            Folio_BPM = folio[2]
            Etapa_cliente = etapa_cliente[1]
            Estatus_cliente = estatus_cliente[4]
            Subetapa_cliente = subetapa_cliente[2]
            Pantalla_cliente = pantalla_cliente[4]
            Macroproceso_cliente = macroproceso_cliente[2]
            Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
            Riesgos_producto = riesgos_producto[0]
            Riesgos_subProducto = riesgos_subProducto[random.randint(0, 4)]

            bulk_indices(id_cliente, id, value_null, SAP_estatus_cliente,
                         SAP_subestatus_cliente, Estatus_seguimientoCartera, Folio_BPM,
                         Etapa_cliente,
                         Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, 1, Macroproceso_cliente,
                         Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente, promotorIt, nombreSAP1,
                         nombreSAP2,
                         apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion, Riesgos_producto,
                         Riesgos_subProducto, Flag_ruta)

        elif id_cliente <= 800 and id_cliente >= 601:
            SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 5)]
            Estatus_seguimientoCartera = estatus_seguimiento_cartera[0]
            Folio_BPM = folio[2]
            Etapa_cliente = etapa_cliente[2]
            Estatus_cliente = estatus_cliente[3]
            Subetapa_cliente = subetapa_cliente[4]
            Pantalla_cliente = pantalla_cliente[5]
            Macroproceso_cliente = macroproceso_cliente[3]
            Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
            Riesgos_producto = riesgos_producto[1]
            Riesgos_subProducto = riesgos_subProducto[random.randint(5, 6)]

            bulk_indices(id_cliente, id, value_null, SAP_estatus_cliente,
                         SAP_subestatus_cliente, Estatus_seguimientoCartera, Folio_BPM,
                         Etapa_cliente,
                         Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, 1, Macroproceso_cliente,
                         Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente, promotorIt, nombreSAP1,
                         nombreSAP2,
                         apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion, Riesgos_producto,
                         Riesgos_subProducto, Flag_ruta)

        elif id_cliente <= 1002 and id_cliente >= 801:
            SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 5)]
            Estatus_seguimientoCartera = estatus_seguimiento_cartera[0]
            Folio_BPM = folio[2]
            Etapa_cliente = etapa_cliente[2]
            Estatus_cliente = estatus_cliente[7]
            Subetapa_cliente = subetapa_cliente[5]
            Pantalla_cliente = pantalla_cliente[6]
            Macroproceso_cliente = macroproceso_cliente[3]
            Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
            Riesgos_producto = riesgos_producto[1]
            Riesgos_subProducto = riesgos_subProducto[random.randint(5, 6)]

            bulk_indices(id_cliente, id, value_null, SAP_estatus_cliente,
                         SAP_subestatus_cliente, Estatus_seguimientoCartera, Folio_BPM,
                         Etapa_cliente,
                         Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, 1, Macroproceso_cliente,
                         Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente, promotorIt, nombreSAP1,
                         nombreSAP2,
                         apellidoSAPPater, apellidoSAPMater, fechaProspeccion, fechaUltimaGestion, Riesgos_producto,
                         Riesgos_subProducto, Flag_ruta)


def bulk_indices(idCliente, id, value_null, Sap_estatus_cliente,
                 Sap_subestatus_cliente, Estatus_seguimientoCartera, Folio, Etapa_cliente,
                 Estatus_cliente, Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, no_contrato,
                 Macroproceso_cliente, Flag_prospecto, SAP_estatus_cliente, SAP_subestatus_cliente,
                 promotorIT, nombreSAP1, nombreSAP2, apellidoSAPPater, apellidoSAPMater, fechaProspeccion,
                 fechaUltimaGestion, Riesgos_producto, Riesgos_subProducto, Flag_ruta):
    doc_renovaciones = build_doc_renovacionestopup(idCliente, id, value_null, Sap_estatus_cliente,
                                                   Sap_subestatus_cliente,
                                                   Estatus_seguimientoCartera, Folio, Etapa_cliente, Estatus_cliente,
                                                   Subetapa_cliente, Folio_Movilidad, Pantalla_cliente, no_contrato,
                                                   Macroproceso_cliente, Flag_prospecto, promotorIT, nombreSAP1,
                                                   nombreSAP2,
                                                   apellidoSAPPater, apellidoSAPMater, fechaProspeccion,
                                                   fechaUltimaGestion, Riesgos_producto, Riesgos_subProducto, Flag_ruta)

    doc_trazabilidad = build_doc_trazabilidad_cliente(idCliente, id, SAP_estatus_cliente, SAP_subestatus_cliente,
                                                      Estatus_seguimientoCartera, Folio, Etapa_cliente, Estatus_cliente,
                                                      Subetapa_cliente, Pantalla_cliente, Macroproceso_cliente,
                                                      no_contrato,
                                                      Folio_Movilidad, Flag_prospecto)

    elasticsearch_opera(doc_renovaciones,doc_trazabilidad)



def elasticsearch_opera(doc_renovaciones, doc_trazabilidad):
    elastic = Elasticsearch()  # conexion local
    # elastic = Elasticsearch(['https://0f015a81ee05196b255efee81a128f42.us-east-1.aws.found.io:9243'],http_auth=('epvazquez', '12345Nmp'))

    elastic = Elasticsearch() #conexion local


    res_renovaciones = elastic.index(index="renovacionestopup_test", doc_type='prospeccion', body=doc_renovaciones)

    res_trazabilidad = elastic.index(index="trazabilidad_cliente_test", doc_type='renovaciones', body=doc_trazabilidad)

    doc_renovacion_id = res_renovaciones['_id']

    doc_trazabilidad_id = res_trazabilidad['_id']

    doc_propierties_renovaciones = {
        "index": {"_index": "renovacionestopup_test", "_type": "prospeccion", "_id": f"{doc_renovacion_id}"}}

    doc_propierties_trazabilidad = {
        "index": {"_index": "trazabilidad_cliente_test", "_type": "renovaciones", "_id": f"{doc_trazabilidad_id}"}}

    print(doc_propierties_renovaciones)
    print(doc_renovaciones)

    file_bulk_renovaciones.write(str(doc_propierties_renovaciones).replace("'", '"') + "\n")
    file_bulk_renovaciones.write(str(doc_renovaciones).replace("'", '"').replace("None", "null") + "\n")

    print(doc_propierties_trazabilidad)
    print(doc_trazabilidad)
    file_bulk_trazabilidad.write(str(doc_propierties_trazabilidad).replace("'", '"') + "\n")
    file_bulk_trazabilidad.write(str(doc_trazabilidad).replace("'", '"').replace("None", "null") + "\n")


def close_json():
    file_bulk_trazabilidad.close()
    file_bulk_renovaciones.close()


elastic_ingest_docs()
close_json()
