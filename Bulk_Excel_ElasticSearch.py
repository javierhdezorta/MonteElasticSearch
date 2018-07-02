from elasticsearch import Elasticsearch
import pandas as pd
import random

folio = [None, 100, 200, 300, 400]

folioMovilidad = [None, 1]

etapa_cliente = ["Prospecto", "Solicitud", "Contratado"]

estatus_cliente = ["Sin Folio", "Proceso", "Aprobada", "Rechazada", "Autorizada", "Agendado", "Firmado", "Pendiente",
                   "Rechazado", "Solventada"]

estatus_seguimiento_cartera = ["en espera", "terminado"]

sap_subestatus_cliente = [42, 43]

subetapa_cliente = ["Sin Gestión", "Pre Solicitud", "Análisis", "Autorización", "Agenda", "Firma"]

macroproceso_cliente = ["Prospección", "Análisis", "Autorización", "Instrumentación"]

pantalla_cliente = ["Pre Gestión", "Ofertas", "Validar información", "Validar documentación", "Autorización", "Agendar",
                    "Consultar agenda"]

autoriza_oferta = ["true", "false"]

flag_prospecto = ["true", "false"]

file = 'Insumo_Ofertas.xlsx'

dataframe = pd.read_excel(file, sheet_name='Ofertas',
                          dtype={'Monto oferta automática_2': str, 'No. de pagos oferta automática_2': str,
                                 'Tasa oferta automática_2': str,
                                 'Monto oferta automática_3': str, 'No. de pagos oferta automática_3': str,
                                 'Tasa oferta automática_3': str,
                                 'Monto oferta automática_4': str, 'No. de pagos oferta automática_4': str,
                                 'Tasa oferta automática_4': str, 'Numero de Prospecto': str, 'Ingreso': str,
                                 'ITA_Monto': str, 'ITA No. de pagos_1': str, 'ITA No. de pagos_2': str,
                                 'ITA No. de pagos_3': str,
                                 'ITA No. de pagos_4': str, 'Monto_0': str,
                                 'No. de pagos oferta automática maxima_1': str,
                                 'No. de pagos oferta automática maxima_2': str,
                                 'No. de pagos oferta automática maxima_3': str,
                                 'No. de pagos oferta automática maxima_4': str, 'Tasa_0': str, 'Contrato': str,
                                 'No. de pagos_0': str, 'Monto oferta automática_1': str,
                                 'No. de pagos oferta automática_1': str,
                                 'Tasa oferta automática_1': str, 'Tasa oferta automática maxima_1': str,
                                 'Tasa oferta automática maxima_2': str,
                                 'Tasa oferta automática maxima_3': str, 'Tasa oferta automática maxima_4': str,
                                 'ITA Tasa_1': str,
                                 'ITA Tasa_2': str, 'ITA Tasa_3': str, 'ITA Tasa_4': str, 'Flag_Ruta': str,
                                 'SAP_telefono1 ': str,
                                 'SAP_telefono2': str, 'fecha_envio': str, 'Pago_0': str,
                                 'Pago oferta automática 1': str,
                                 'Pago oferta automática 2': str, 'Pago oferta automática 3': str,
                                 'Pago oferta automática 4': str,
                                 'Pago oferta automática maxima_1': str, 'Pago oferta automática maxima_2': str,
                                 'Pago oferta automática maxima_3': str,
                                 'Pago oferta automática maxima_4': str, 'Monto oferta automática maxima 3': str,
                                 'Monto oferta automática maxima_1': str,
                                 'Monto oferta automática maxima_2': str, 'Monto oferta automática maxima_3': str,
                                 'Monto oferta automática maxima_4': str,
                                 'Pago oferta automática_2': str, 'Pago oferta automática_3': str,
                                 'Pago oferta automática_4': str})


#print(dataframe.dtypes)


def build_doc_renovacionestopup(Numero_de_Prospecto, Contrato, Producto, Subproducto, No_de_pagos_0, Tasa_0, Monto_0,
                                Pago_0, NR_0,
                                Sub_Producto_a_ofertar, Ingreso, Monto_oferta_automatica_1,
                                No_de_pagos_oferta_automatica_1,
                                Tasa_oferta_automatica_1, Pago_oferta_automatica_1, Monto_oferta_automatica_2,
                                No_de_pagos_oferta_automatica_2,
                                Tasa_oferta_automatica_2, Pago_oferta_automatica_2, Monto_oferta_automatica_3,
                                No_de_pagos_oferta_automatica_3,
                                Tasa_oferta_automatica_3, Pago_oferta_automatica_3, Monto_oferta_automatica_4,
                                No_de_pagos_oferta_automatica_4,
                                Tasa_oferta_automatica_4, Pago_oferta_automatica_4, Monto_oferta_automatica_maxima_1,
                                No_de_pagos_oferta_automatica_maxima_1,
                                Tasa_oferta_automatica_maxima_1, Pago_oferta_automatica_maxima_1,
                                Monto_oferta_automatica_maxima_2, No_de_pagos_oferta_automatica_maxima_2,
                                Tasa_oferta_automatica_maxima_2, Pago_oferta_automatica_maxima_2,
                                Monto_oferta_automatica_maxima_3, No_de_pagos_oferta_automatica_maxima_3,
                                Tasa_oferta_automatica_maxima_3, Pago_oferta_automatica_maxima_3,
                                Monto_oferta_automatica_maxima_4, No_de_pagos_oferta_automatica_maxima_4,
                                Tasa_oferta_automatica_maxima_4, Pago_oferta_automatica_maxima_4, ITA_Tasa_1,
                                ITA_Tasa_2, ITA_Tasa_3, ITA_Tasa_4, ITA_No_de_pagos_1,
                                ITA_No_de_pagos_2, ITA_No_de_pagos_3, ITA_No_de_pagos_4, NR_1, Flag_Ruta, fecha_envio,
                                SAP_estatus_cliente, SAP_subestatus_cliente,
                                Estatus_seguimientoCartera, Folio, Etapa_cliente, Estatus_cliente, Folio_Movilidad,
                                Subetapa_cliente, Pantalla_cliente, Macroproceso_cliente, Flag_prospecto,
                                SAP_telefono1, SAP_telefono2, id, ITA_Monto):
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
        "ITA_monto": ITA_Monto,
        "ITA_numeroPagos_1": ITA_No_de_pagos_1,
        "ITA_numeroPagos_2": ITA_No_de_pagos_2,
        "ITA_numeroPagos_3": ITA_No_de_pagos_3,
        "ITA_numeroPagos_4": ITA_No_de_pagos_4,
        "SAP_apellidoMaterno_cliente": "Jackson",
        "SAP_apellidoPaterno_cliente": "Jackson",
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
        "SAP_estatus_cliente": SAP_estatus_cliente,
        "SAP_fecha_contacto_telefono_1": "2018-05-03T13:42:20.760",
        "SAP_fecha_contacto_telefono_2": "2018-05-03T13:42:20.760",
        "SAP_geolocalizacion_x_domicilio": "XDomi",
        "SAP_geolocalizacion_y_domicilio": "YDomi",
        "SAP_intentos_telefono_1": 1,
        "SAP_intentos_telefono_2": 2,
        "SAP_municipioDomicilio": "MuniDomi",
        "SAP_nombre_cliente_1": "NombreCliente",
        "SAP_nombre_cliente_2": "NombreCliete2",
        "SAP_numeroDomicilio_cliente": "12",
        "SAP_producto": "Producto",
        "SAP_subProducto": "Renovacion",
        "SAP_subestatus_cliente": SAP_subestatus_cliente,
        "SAP_telefono_1": SAP_telefono1,
        "SAP_telefono_2": SAP_telefono2,
        "SAP_tipoContacto_telefono_1": "tipoContacto",
        "SAP_tipoContacto_telefono_2": "tipoContacto",
        "TP_telefono_tipificacacion": "55891283312",
        "TP_telefono_tipificacion_etiqueta": "1231234123",
        "TP_tipo_tipificacion_telefono": "tipoTipificacion",
        "contrato": Contrato,
        "curp_cliente": "AARM981209HMCMK09",
        "detalle_oferta": "Renovacion",
        "estatus_campania": "1",
        "estatus_carga": "1",
        "estatus_seguimientoCartera": Estatus_seguimientoCartera,
        "etiqueta_contacto_telefonos_actualizados": "contactado",
        "etiqueta_oferta": "etiqueta oferta",
        "etiqueta_telefonos_actualizados": "Etiqueta2",
        "fecha_activacion_campania": "2018-05-03T13:42:20.760",
        "fecha_cargaArchivo_layoutOfertas": fecha_envio[0:4] + "-" + fecha_envio[4:6] + "-" + fecha_envio[6:8],
        "fecha_insert": "2018-05-03T13:42:20.760",
        "fecha_prospeccion": "2018-05-03T13:42:20.760",
        "flag_ruta": Flag_Ruta,
        "generadoUsuario_descripcionContacto_telefono": "contacto",
        "generadoUsuario_extension_telefono": "2212",
        "generadoUsuario_fecha_contacto_telefono": "2018-05-03T13:42:20.760",
        "generadoUsuario_intentosContacto_telefono": 4,
        "generadoUsuario_telefono": "5512982391",
        "generadoUsuario_tipo_telefono": "tipoGenerado",
        "gerenciadeVentas": "gerenciaVentas",
        "idCliente": Numero_de_Prospecto,
        # "id_cliente": Numero_de_Prospecto,
        "idProspeccion": id,
        "id_campania": id,
        "ingreso": Ingreso,
        "localidad_cliente": "Mexico",
        "montoNuevaOfertaAutomatica": 100,
        "montoNuevaOfertaAutomaticaMaxima": None,
        "montoOfertaAutomaticaMaxima_1": Monto_oferta_automatica_maxima_1,
        "montoOfertaAutomaticaMaxima_2": Monto_oferta_automatica_maxima_2,
        "montoOfertaAutomaticaMaxima_3": Monto_oferta_automatica_maxima_3,
        "montoOfertaAutomaticaMaxima_4": Monto_oferta_automatica_maxima_4,
        "montoOfertaAutomatica_1": Monto_oferta_automatica_1,
        "montoOfertaAutomatica_2": Monto_oferta_automatica_2,
        "montoOfertaAutomatica_3": Monto_oferta_automatica_3,
        "montoOfertaAutomatica_4": Monto_oferta_automatica_4,
        "montoOfertaRequeridoProspecto": 100,
        "monto_producto_actual": Monto_0,
        "motivo_oferta": "NA",
        "nivel_riesgo_1": NR_1,
        "nivel_riesgo_actual": NR_0,
        "nombre_lista_marketing": "12082018_TCCP",
        "numeroPagosNuevaOfertaAutomatica": 100,
        "numeroPagosNuevaOfertaAutomaticaMaxima": 200,
        "numeroPagosOfertaAutomaticaMaxima_1": No_de_pagos_oferta_automatica_maxima_1,
        "numeroPagosOfertaAutomaticaMaxima_2": No_de_pagos_oferta_automatica_maxima_2,
        "numeroPagosOfertaAutomaticaMaxima_3": No_de_pagos_oferta_automatica_maxima_3,
        "numeroPagosOfertaAutomaticaMaxima_4": No_de_pagos_oferta_automatica_maxima_4,
        "numeroPagosOfertaAutomatica_1": No_de_pagos_oferta_automatica_1,
        "numeroPagosOfertaAutomatica_2": No_de_pagos_oferta_automatica_2,
        "numeroPagosOfertaAutomatica_3": No_de_pagos_oferta_automatica_3,
        "numeroPagosOfertaAutomatica_4": No_de_pagos_oferta_automatica_4,
        "numeroPagosOfertaRequeridoProspecto": 2,
        "numeroPagos_producto_actual": No_de_pagos_0,
        "ofertaSeleccionada": "StringPropiedadesOfertaSeleccionada",
        "pagoNuevaOfertaAutomatica": 100,
        "pagoNuevaOfertaAutomaticaMaxima": 200,
        "pagoOfertaAutomaticaMaxima_1": Pago_oferta_automatica_maxima_1,
        "pagoOfertaAutomaticaMaxima_2": Pago_oferta_automatica_maxima_2,
        "pagoOfertaAutomaticaMaxima_3": Pago_oferta_automatica_maxima_3,
        "pagoOfertaAutomaticaMaxima_4": Pago_oferta_automatica_maxima_4,
        "pagoOfertaAutomatica_1": Pago_oferta_automatica_1,
        "pagoOfertaAutomatica_2": Pago_oferta_automatica_2,
        "pagoOfertaAutomatica_3": Pago_oferta_automatica_3,
        "pagoOfertaAutomatica_4": Pago_oferta_automatica_4,
        "pagoOfertaRequeridoProspecto": 1290,
        "pago_producto_actual": Pago_0,
        "promotorApellidos": "Salazar",
        "promotorNombre": "Alejandro",
        "rfc_cliente": "EABM091218S06",
        "riesgos_producto": Producto,
        "riesgos_subProducto": Subproducto,
        "seguimientoCartera": "cartera",
        "subProductoOferta": Sub_Producto_a_ofertar,
        "tasaNuevaOfertaAutomatica": 100,
        "tasaNuevaOfertaAutomaticaMaxima": 200,
        "tasaOfertaAutomaticaMaxima_1": Tasa_oferta_automatica_maxima_1,
        "tasaOfertaAutomaticaMaxima_2": Tasa_oferta_automatica_maxima_2,
        "tasaOfertaAutomaticaMaxima_3": Tasa_oferta_automatica_maxima_3,
        "tasaOfertaAutomaticaMaxima_4": Tasa_oferta_automatica_maxima_4,
        "tasaOfertaAutomatica_1": Tasa_oferta_automatica_1,
        "tasaOfertaAutomatica_2": Tasa_oferta_automatica_2,
        "tasaOfertaAutomatica_3": Tasa_oferta_automatica_3,
        "tasaOfertaAutomatica_4": Tasa_oferta_automatica_4,
        "tasaOfertaRequeridoProspecto": 12,
        "tasa_producto_actual": Tasa_0,
        "telefonos_actualizados": "5512901293@5590982378@551232467",
        "tipo_telefono_actualizados": "casa@oficina@departamento",
        "duracion_campania": 5,
        "folio_BPM": Folio,
        "etapa_cliente": Etapa_cliente,
        "estatus_cliente": Estatus_cliente,
        "ITA_tasa_1": ITA_Tasa_1,
        "ITA_tasa_2": ITA_Tasa_2,
        "ITA_tasa_3": ITA_Tasa_3,
        "ITA_tasa_4": ITA_Tasa_4,
        "fecha_nacimiento_cliente": "2018-09-03T13:42:20.760",
        "fecha_ultima_gestion_cliente": "2018-03-03T13:42:20.760",
        "folio_movilidad": Folio_Movilidad,
        "subetapa_cliente": Subetapa_cliente,
        "autoriza_oferta": autoriza_oferta[random.randint(0, 1)],
        "pantalla_cliente": Pantalla_cliente,
        "numero_solicitud": None,
        "numeroContrato": "",
        "macroproceso_cliente": Macroproceso_cliente,
        "flag_prospecto": Flag_prospecto,
        "idPromotor": random.randint(1, 100),
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


def build_doc_trazabilidad_cliente(Numero_de_Prospecto, Contrato, Etapa_cliente, Estatus_cliente, Subetapa_cliente,
                                   Macroproceso_cliente, Pantalla_cliente, Folio,
                                   Folio_Movilidad, Estatus_seguimientoCartera, SAP_estatus_cliente,
                                   SAP_subestatus_cliente, Flag_prospecto, id):  # valueNull, value
    doc = {
        "etapa_cliente": Etapa_cliente,
        "estatus_cliente": Estatus_cliente,
        "subetapa_cliente": Subetapa_cliente,
        "macroproceso_cliente": Macroproceso_cliente,
        "pantalla_cliente": Pantalla_cliente,
        "folio_BPM": Folio,
        "folio_movilidad": Folio_Movilidad,
        "numero_solicitud": None,
        "numeroContrato": Contrato,
        "idCliente": Numero_de_Prospecto,  # id_cliente
        # "id_cliente": Numero_de_Prospecto,
        "idProspeccion": id,  # id_prospeccion
        "id_campania": id,
        # "fecha_cambio_estatus": "2018-05-03T13:42:20.760",
        "fecha_ultima_gestion_cliente": "2018-05-03T13:42:20.760",  # fecha_cambio_estatus  fecha_ultima_gestion_cliente
        "seguimientoCartera": "SeguimientoCartera",
        "estatus_seguimientoCartera": Estatus_seguimientoCartera,
        "SAP_estatus_cliente": SAP_estatus_cliente,
        "SAP_subestatus_cliente": SAP_subestatus_cliente,
        "flag_prospecto": Flag_prospecto
    }
    return doc


def elastic_ingest_docs():
    id = 1
    for index, row in dataframe.iterrows():
        Numero_de_ProspectoPre = str(row["Numero de Prospecto"]).strip()
        Numero_de_Prospecto = Numero_de_ProspectoPre if (Numero_de_ProspectoPre != 'nan') else None

        ContratoPre = str(row["Contrato"]).strip()
        Contrato = ContratoPre if (ContratoPre != 'nan') else None

        ProductoPre = str(row["Producto"]).strip()
        Producto = ProductoPre if (ProductoPre != 'nan') else None

        SubproductoPre = str(row["Subproducto"]).strip()
        Subproducto = SubproductoPre if (SubproductoPre != 'nan') else None

        No_de_pagos_0Pre = str(row["No. de pagos_0"]).strip()
        No_de_pagos_0 = No_de_pagos_0Pre if (No_de_pagos_0Pre != 'nan') else None

        Tasa_0Pre = str(row["Tasa_0"]).strip()
        Tasa_0 = Tasa_0Pre if (Tasa_0Pre != 'nan') else None

        Monto_0Pre = str(row["Monto_0"]).strip()
        Monto_0 = Monto_0Pre if (Monto_0Pre != 'nan') else None

        Pago_0Pre = str(row["Pago_0"]).strip()
        Pago_0 = Pago_0Pre if (Pago_0Pre != 'nan') else None

        NR_0Pre = str(row["NR_0"]).strip()
        NR_0 = NR_0Pre if (NR_0Pre != 'nan') else None

        Sub_Producto_a_ofertarPre = str(row["Sub Producto a ofertar"]).strip()
        Sub_Producto_a_ofertar = Sub_Producto_a_ofertarPre if (Sub_Producto_a_ofertarPre != 'nan') else None

        IngresoPre = str(row["Ingreso"]).strip()
        Ingreso = IngresoPre if (IngresoPre != 'nan') else None

        Monto_oferta_automatica_1Pre = str(row["Monto oferta automática_1"]).strip()
        Monto_oferta_automatica_1 = Monto_oferta_automatica_1Pre if (Monto_oferta_automatica_1Pre != 'nan') else None

        No_de_pagos_oferta_automatica_1Pre = str(row["No. de pagos oferta automática_1"]).strip()
        No_de_pagos_oferta_automatica_1 = No_de_pagos_oferta_automatica_1Pre if (
                No_de_pagos_oferta_automatica_1Pre != 'nan') else None

        Tasa_oferta_automatica_1Pre = str(row["Tasa oferta automática_1"]).strip()
        Tasa_oferta_automatica_1 = Tasa_oferta_automatica_1Pre if (Tasa_oferta_automatica_1Pre != 'nan') else None

        Pago_oferta_automatica_1Pre = str(row["Pago oferta automática 1"]).strip()
        Pago_oferta_automatica_1 = Pago_oferta_automatica_1Pre if (Pago_oferta_automatica_1Pre != 'nan') else None

        Monto_oferta_automatica_2Pre = str(row["Monto oferta automática_2"]).strip()
        Monto_oferta_automatica_2 = Monto_oferta_automatica_2Pre if (Monto_oferta_automatica_2Pre != 'nan') else None

        No_de_pagos_oferta_automatica_2Pre = str(row["No. de pagos oferta automática_2"]).strip()
        No_de_pagos_oferta_automatica_2 = No_de_pagos_oferta_automatica_2Pre if (
                No_de_pagos_oferta_automatica_2Pre != 'nan') else None

        Tasa_oferta_automatica_2Pre = str(row["Tasa oferta automática_2"]).strip()
        Tasa_oferta_automatica_2 = Tasa_oferta_automatica_2Pre if (Tasa_oferta_automatica_2Pre != 'nan') else None

        Pago_oferta_automatica_2Pre = str(row["Pago oferta automática_2"]).strip()
        Pago_oferta_automatica_2 = Pago_oferta_automatica_2Pre if (Pago_oferta_automatica_2Pre != 'nan') else None

        Tasa_oferta_automatica_3Pre = str(row["Tasa oferta automática_3"]).strip()
        Tasa_oferta_automatica_3 = Tasa_oferta_automatica_3Pre if (Tasa_oferta_automatica_3Pre != 'nan') else None

        Monto_oferta_automatica_3Pre = str(row["Monto oferta automática_3"]).strip()
        Monto_oferta_automatica_3 = Monto_oferta_automatica_3Pre if (Monto_oferta_automatica_3Pre != 'nan') else None

        No_de_pagos_oferta_automatica_3Pre = str(row["No. de pagos oferta automática_3"]).strip()
        No_de_pagos_oferta_automatica_3 = No_de_pagos_oferta_automatica_3Pre if (
                No_de_pagos_oferta_automatica_3Pre != 'nan') else None

        Pago_oferta_automatica_3Pre = str(row["Pago oferta automática_3"]).strip()
        Pago_oferta_automatica_3 = Pago_oferta_automatica_3Pre if (Pago_oferta_automatica_3Pre != 'nan') else None

        Monto_oferta_automatica_4Pre = str(row["Monto oferta automática_4"]).strip()
        Monto_oferta_automatica_4 = Monto_oferta_automatica_4Pre if (Monto_oferta_automatica_4Pre != 'nan') else None

        No_de_pagos_oferta_automatica_4Pre = str(row["No. de pagos oferta automática_4"]).strip()
        No_de_pagos_oferta_automatica_4 = No_de_pagos_oferta_automatica_4Pre if (
                No_de_pagos_oferta_automatica_4Pre != 'nan') else None

        Tasa_oferta_automatica_4Pre = str(row["Tasa oferta automática_4"]).strip()
        Tasa_oferta_automatica_4 = Tasa_oferta_automatica_4Pre if (Tasa_oferta_automatica_4Pre != 'nan') else None

        Pago_oferta_automatica_4Pre = str(row["Pago oferta automática_4"]).strip()
        Pago_oferta_automatica_4 = Pago_oferta_automatica_4Pre if (Pago_oferta_automatica_4Pre != 'nan') else None

        Monto_oferta_automatica_maxima_1Pre = str(row["Monto oferta automática maxima_1"]).strip()
        Monto_oferta_automatica_maxima_1 = Monto_oferta_automatica_maxima_1Pre if (
                Monto_oferta_automatica_maxima_1Pre != 'nan') else None

        No_de_pagos_oferta_automatica_maxima_1Pre = str(row["No. de pagos oferta automática maxima_1"]).strip()
        No_de_pagos_oferta_automatica_maxima_1 = No_de_pagos_oferta_automatica_maxima_1Pre if (
                No_de_pagos_oferta_automatica_maxima_1Pre != 'nan') else None

        Tasa_oferta_automatica_maxima_1Pre = str(row["Tasa oferta automática maxima_1"]).strip()
        Tasa_oferta_automatica_maxima_1 = Tasa_oferta_automatica_maxima_1Pre if (
                Tasa_oferta_automatica_maxima_1Pre != 'nan') else None

        Pago_oferta_automatica_maxima_1Pre = str(row["Pago oferta automática maxima_1"]).strip()
        Pago_oferta_automatica_maxima_1 = Pago_oferta_automatica_maxima_1Pre if (
                Pago_oferta_automatica_maxima_1Pre != 'nan') else None

        Monto_oferta_automatica_maxima_2Pre = str(row["Monto oferta automática maxima_2"]).strip()
        Monto_oferta_automatica_maxima_2 = Monto_oferta_automatica_maxima_2Pre if (
                Monto_oferta_automatica_maxima_2Pre != 'nan') else None

        No_de_pagos_oferta_automatica_maxima_2Pre = str(row["No. de pagos oferta automática maxima_2"]).strip()
        No_de_pagos_oferta_automatica_maxima_2 = No_de_pagos_oferta_automatica_maxima_2Pre if (
                No_de_pagos_oferta_automatica_maxima_2Pre != 'nan') else None

        Tasa_oferta_automatica_maxima_2Pre = str(row["Tasa oferta automática maxima_2"]).strip()
        Tasa_oferta_automatica_maxima_2 = Tasa_oferta_automatica_maxima_2Pre if (
                Tasa_oferta_automatica_maxima_2Pre != 'nan') else None

        Pago_oferta_automatica_maxima_2Pre = str(row["Pago oferta automática maxima_2"]).strip()
        Pago_oferta_automatica_maxima_2 = Pago_oferta_automatica_maxima_2Pre if (
                Pago_oferta_automatica_maxima_2Pre != 'nan') else None

        Monto_oferta_automatica_maxima_3Pre = str(row["Monto oferta automática maxima_3"]).strip()
        Monto_oferta_automatica_maxima_3 = Monto_oferta_automatica_maxima_3Pre if (
                Monto_oferta_automatica_maxima_3Pre != 'nan') else None

        No_de_pagos_oferta_automatica_maxima_3Pre = str(row["No. de pagos oferta automática maxima_3"]).strip().replace(
            ".0", "")
        No_de_pagos_oferta_automatica_maxima_3 = No_de_pagos_oferta_automatica_maxima_3Pre if (
                No_de_pagos_oferta_automatica_maxima_3Pre != 'nan') else None

        Tasa_oferta_automatica_maxima_3Pre = str(row["Tasa oferta automática maxima_3"]).strip()
        Tasa_oferta_automatica_maxima_3 = Tasa_oferta_automatica_maxima_3Pre if (
                Tasa_oferta_automatica_maxima_3Pre != 'nan') else None

        Pago_oferta_automatica_maxima_3Pre = str(row["Pago oferta automática maxima_3"]).strip()
        Pago_oferta_automatica_maxima_3 = Pago_oferta_automatica_maxima_3Pre if (
                Pago_oferta_automatica_maxima_3Pre != 'nan') else None

        Monto_oferta_automatica_maxima_4Pre = str(row["Monto oferta automática maxima_4"]).strip()
        Monto_oferta_automatica_maxima_4 = Monto_oferta_automatica_maxima_4Pre if (
                Monto_oferta_automatica_maxima_4Pre != 'nan') else None

        No_de_pagos_oferta_automatica_maxima_4Pre = str(row["No. de pagos oferta automática maxima_4"]).strip().replace(
            ".0", "")
        No_de_pagos_oferta_automatica_maxima_4 = No_de_pagos_oferta_automatica_maxima_4Pre if (
                No_de_pagos_oferta_automatica_maxima_4Pre != 'nan') else None

        Tasa_oferta_automatica_maxima_4Pre = str(row["Tasa oferta automática maxima_4"]).strip()
        Tasa_oferta_automatica_maxima_4 = Tasa_oferta_automatica_maxima_4Pre if (
                Tasa_oferta_automatica_maxima_4Pre != 'nan') else None

        Pago_oferta_automatica_maxima_4Pre = str(row["Pago oferta automática maxima_4"]).strip()
        Pago_oferta_automatica_maxima_4 = Pago_oferta_automatica_maxima_4Pre if (
                Pago_oferta_automatica_maxima_4Pre != 'nan') else None

        ITA_Tasa_1Pre = str(row["ITA Tasa_1"]).strip()
        ITA_Tasa_1 = ITA_Tasa_1Pre if (ITA_Tasa_1Pre != 'nan') else None

        ITA_Tasa_2Pre = str(row["ITA Tasa_2"]).strip()
        ITA_Tasa_2 = ITA_Tasa_2Pre if (ITA_Tasa_2Pre != 'nan') else None

        ITA_Tasa_3Pre = str(row["ITA Tasa_3"]).strip()
        ITA_Tasa_3 = ITA_Tasa_3Pre if (ITA_Tasa_3Pre != 'nan') else None

        ITA_Tasa_4Pre = str(row["ITA Tasa_4"]).strip()
        ITA_Tasa_4 = ITA_Tasa_4Pre if (ITA_Tasa_4Pre != 'nan') else None

        ITA_MontoPre = str(row["ITA_Monto"]).strip()
        ITA_Monto = ITA_MontoPre if (ITA_MontoPre != 'nan') else None

        ITA_No_de_pagos_1Pre = str(row["ITA No. de pagos_1"]).strip()
        ITA_No_de_pagos_1 = ITA_No_de_pagos_1Pre if (ITA_No_de_pagos_1Pre != 'nan') else None

        ITA_No_de_pagos_2Pre = str(row["ITA No. de pagos_2"]).strip()
        ITA_No_de_pagos_2 = ITA_No_de_pagos_2Pre if (ITA_No_de_pagos_2Pre != 'nan') else None

        ITA_No_de_pagos_3Pre = str(row["ITA No. de pagos_3"]).strip()
        ITA_No_de_pagos_3 = ITA_No_de_pagos_3Pre if (ITA_No_de_pagos_3Pre != 'nan') else None

        ITA_No_de_pagos_4Pre = str(row["ITA No. de pagos_4"]).strip()
        ITA_No_de_pagos_4 = ITA_No_de_pagos_4Pre if (ITA_No_de_pagos_4Pre != 'nan') else None

        NR_1Pre = str(row["NR_1"]).strip()
        NR_1 = NR_1Pre if (NR_1Pre != 'nan') else None

        Flag_RutaPre = str(row["Flag_Ruta"]).strip()
        Flag_Ruta = Flag_RutaPre if (Flag_RutaPre != 'nan') else None

        fecha_envioPre = str(row["fecha_envio"]).strip()
        fecha_envio = fecha_envioPre if (fecha_envioPre != 'nan') else None

        SAP_telefono1Pre = str(row["SAP_telefono1 "]).strip()
        SAP_telefono1 = SAP_telefono1Pre if (SAP_telefono1Pre != 'nan') else None

        SAP_telefono2Pre = str(row["SAP_telefono2"]).strip()
        SAP_telefono2 = SAP_telefono2Pre if (SAP_telefono2Pre != 'nan') else None

        Etapa_cliente = etapa_cliente[random.randint(0, 2)]
        Estatus_cliente = estatus_cliente[random.randint(0, 2)]
        Subetapa_cliente = subetapa_cliente[random.randint(0, 2)]
        Macroproceso_cliente = macroproceso_cliente[random.randint(0, 2)]
        Pantalla_cliente = pantalla_cliente[random.randint(0, 2)]
        Folio = folio[random.randint(0, 2)]
        Folio_Movilidad = folioMovilidad[random.randint(0, 1)]
        Flag_prospecto = "true"
        Estatus_seguimientoCartera = estatus_seguimiento_cartera[random.randint(0, 1)]
        SAP_estatus_cliente = "37"
        SAP_subestatus_cliente = sap_subestatus_cliente[random.randint(0, 1)]

        bulk_indices(Numero_de_Prospecto, Contrato, Producto, Subproducto,
                     No_de_pagos_0, Tasa_0, Monto_0, Pago_0, NR_0,
                     Sub_Producto_a_ofertar, Ingreso, Monto_oferta_automatica_1,
                     No_de_pagos_oferta_automatica_1
                     , Tasa_oferta_automatica_1, Pago_oferta_automatica_1,
                     Monto_oferta_automatica_2, No_de_pagos_oferta_automatica_2,
                     Tasa_oferta_automatica_2, Pago_oferta_automatica_2,
                     Monto_oferta_automatica_3
                     , No_de_pagos_oferta_automatica_3, Tasa_oferta_automatica_3,
                     Pago_oferta_automatica_3, Monto_oferta_automatica_4,
                     No_de_pagos_oferta_automatica_4, Tasa_oferta_automatica_4,
                     Pago_oferta_automatica_4
                     , Monto_oferta_automatica_maxima_1,
                     No_de_pagos_oferta_automatica_maxima_1,
                     Tasa_oferta_automatica_maxima_1,
                     Pago_oferta_automatica_maxima_1,
                     Monto_oferta_automatica_maxima_2,
                     No_de_pagos_oferta_automatica_maxima_2
                     , Tasa_oferta_automatica_maxima_2,
                     Pago_oferta_automatica_maxima_2,
                     Monto_oferta_automatica_maxima_3,
                     No_de_pagos_oferta_automatica_maxima_3,
                     Tasa_oferta_automatica_maxima_3
                     , Pago_oferta_automatica_maxima_3,
                     Monto_oferta_automatica_maxima_4,
                     No_de_pagos_oferta_automatica_maxima_4,
                     Tasa_oferta_automatica_maxima_4, Pago_oferta_automatica_maxima_4
                     , ITA_Tasa_1, ITA_Tasa_2, ITA_Tasa_3, ITA_Tasa_4,
                     ITA_No_de_pagos_1, ITA_No_de_pagos_2, ITA_No_de_pagos_3,
                     ITA_No_de_pagos_4, NR_1, Flag_Ruta, fecha_envio,
                     SAP_estatus_cliente, SAP_subestatus_cliente
                     , Estatus_seguimientoCartera, Folio, Etapa_cliente,
                     Estatus_cliente, Folio_Movilidad, Subetapa_cliente,
                     Pantalla_cliente, Macroproceso_cliente, Flag_prospecto,
                     SAP_telefono1, SAP_telefono2, id, ITA_Monto)


def bulk_indices(Numero_de_Prospecto, Contrato, Producto, Subproducto,
                 No_de_pagos_0, Tasa_0, Monto_0, Pago_0, NR_0,
                 Sub_Producto_a_ofertar, Ingreso, Monto_oferta_automatica_1,
                 No_de_pagos_oferta_automatica_1
                 , Tasa_oferta_automatica_1, Pago_oferta_automatica_1,
                 Monto_oferta_automatica_2, No_de_pagos_oferta_automatica_2,
                 Tasa_oferta_automatica_2, Pago_oferta_automatica_2,
                 Monto_oferta_automatica_3
                 , No_de_pagos_oferta_automatica_3, Tasa_oferta_automatica_3,
                 Pago_oferta_automatica_3, Monto_oferta_automatica_4,
                 No_de_pagos_oferta_automatica_4, Tasa_oferta_automatica_4,
                 Pago_oferta_automatica_4
                 , Monto_oferta_automatica_maxima_1,
                 No_de_pagos_oferta_automatica_maxima_1,
                 Tasa_oferta_automatica_maxima_1,
                 Pago_oferta_automatica_maxima_1,
                 Monto_oferta_automatica_maxima_2,
                 No_de_pagos_oferta_automatica_maxima_2
                 , Tasa_oferta_automatica_maxima_2,
                 Pago_oferta_automatica_maxima_2,
                 Monto_oferta_automatica_maxima_3,
                 No_de_pagos_oferta_automatica_maxima_3,
                 Tasa_oferta_automatica_maxima_3
                 , Pago_oferta_automatica_maxima_3,
                 Monto_oferta_automatica_maxima_4,
                 No_de_pagos_oferta_automatica_maxima_4,
                 Tasa_oferta_automatica_maxima_4, Pago_oferta_automatica_maxima_4
                 , ITA_Tasa_1, ITA_Tasa_2, ITA_Tasa_3, ITA_Tasa_4,
                 ITA_No_de_pagos_1, ITA_No_de_pagos_2, ITA_No_de_pagos_3,
                 ITA_No_de_pagos_4, NR_1, Flag_Ruta, fecha_envio,
                 SAP_estatus_cliente, SAP_subestatus_cliente
                 , Estatus_seguimientoCartera, Folio, Etapa_cliente,
                 Estatus_cliente, Folio_Movilidad, Subetapa_cliente,
                 Pantalla_cliente, Macroproceso_cliente, Flag_prospecto,
                 SAP_telefono1, SAP_telefono2, id, ITA_Monto):


    if Numero_de_Prospecto != None:
        doc_renovaciones = build_doc_renovacionestopup(Numero_de_Prospecto, Contrato, Producto, Subproducto,
                                                       No_de_pagos_0, Tasa_0, Monto_0, Pago_0, NR_0,
                                                       Sub_Producto_a_ofertar, Ingreso, Monto_oferta_automatica_1,
                                                       No_de_pagos_oferta_automatica_1
                                                       , Tasa_oferta_automatica_1, Pago_oferta_automatica_1,
                                                       Monto_oferta_automatica_2, No_de_pagos_oferta_automatica_2,
                                                       Tasa_oferta_automatica_2, Pago_oferta_automatica_2,
                                                       Monto_oferta_automatica_3
                                                       , No_de_pagos_oferta_automatica_3, Tasa_oferta_automatica_3,
                                                       Pago_oferta_automatica_3, Monto_oferta_automatica_4,
                                                       No_de_pagos_oferta_automatica_4, Tasa_oferta_automatica_4,
                                                       Pago_oferta_automatica_4
                                                       , Monto_oferta_automatica_maxima_1,
                                                       No_de_pagos_oferta_automatica_maxima_1,
                                                       Tasa_oferta_automatica_maxima_1,
                                                       Pago_oferta_automatica_maxima_1,
                                                       Monto_oferta_automatica_maxima_2,
                                                       No_de_pagos_oferta_automatica_maxima_2
                                                       , Tasa_oferta_automatica_maxima_2,
                                                       Pago_oferta_automatica_maxima_2,
                                                       Monto_oferta_automatica_maxima_3,
                                                       No_de_pagos_oferta_automatica_maxima_3,
                                                       Tasa_oferta_automatica_maxima_3
                                                       , Pago_oferta_automatica_maxima_3,
                                                       Monto_oferta_automatica_maxima_4,
                                                       No_de_pagos_oferta_automatica_maxima_4,
                                                       Tasa_oferta_automatica_maxima_4, Pago_oferta_automatica_maxima_4
                                                       , ITA_Tasa_1, ITA_Tasa_2, ITA_Tasa_3, ITA_Tasa_4,
                                                       ITA_No_de_pagos_1, ITA_No_de_pagos_2, ITA_No_de_pagos_3,
                                                       ITA_No_de_pagos_4, NR_1, Flag_Ruta, fecha_envio,
                                                       SAP_estatus_cliente, SAP_subestatus_cliente
                                                       , Estatus_seguimientoCartera, Folio, Etapa_cliente,
                                                       Estatus_cliente, Folio_Movilidad, Subetapa_cliente,
                                                       Pantalla_cliente, Macroproceso_cliente, Flag_prospecto,
                                                       SAP_telefono1, SAP_telefono2, id, ITA_Monto)

        doc_trazabilidad = build_doc_trazabilidad_cliente(Numero_de_Prospecto, Contrato, Etapa_cliente,
                                                          Estatus_cliente, Subetapa_cliente, Macroproceso_cliente,
                                                          Pantalla_cliente, Folio, Folio_Movilidad,
                                                          Estatus_seguimientoCartera
                                                          , SAP_estatus_cliente, SAP_subestatus_cliente,
                                                          Flag_prospecto, id)

        elasticsearch_opera(doc_renovaciones, doc_trazabilidad,Numero_de_Prospecto)


def elasticsearch_opera(doc_renovaciones, doc_trazabilidad, Numero_de_Prospecto):
    '''elastic = Elasticsearch(['https://0f015a81ee05196b255efee81a128f42.us-east-1.aws.found.io:9243'],
                            http_auth=('epvazquez', '12345Nmp'))  # conexion a server'''
    elastic = Elasticsearch() #conexion local
    elastic.delete_by_query(index="renovacionestopup_test", doc_type='prospeccion', body={"query":{"match": {"idCliente": f"{Numero_de_Prospecto}"}}})
    elastic.delete_by_query(index="trazabilidad_cliente_test", doc_type='renovaciones', body={"query":{"match": {"idCliente": f"{Numero_de_Prospecto}"}}})

    doc_renovaciones = build_doc_renovacionestopup(Numero_de_Prospecto, Contrato, Producto, Subproducto,
                                                   No_de_pagos_0, Tasa_0, Monto_0, Pago_0, NR_0,
                                                   Sub_Producto_a_ofertar, Ingreso, Monto_oferta_automatica_1,
                                                   No_de_pagos_oferta_automatica_1
                                                   , Tasa_oferta_automatica_1, Pago_oferta_automatica_1,
                                                   Monto_oferta_automatica_2, No_de_pagos_oferta_automatica_2,
                                                   Tasa_oferta_automatica_2, Pago_oferta_automatica_2,
                                                   Monto_oferta_automatica_3
                                                   , No_de_pagos_oferta_automatica_3, Tasa_oferta_automatica_3,
                                                   Pago_oferta_automatica_3, Monto_oferta_automatica_4,
                                                   No_de_pagos_oferta_automatica_4, Tasa_oferta_automatica_4,
                                                   Pago_oferta_automatica_4
                                                   , Monto_oferta_automatica_maxima_1,
                                                   No_de_pagos_oferta_automatica_maxima_1,
                                                   Tasa_oferta_automatica_maxima_1,
                                                   Pago_oferta_automatica_maxima_1,
                                                   Monto_oferta_automatica_maxima_2,
                                                   No_de_pagos_oferta_automatica_maxima_2
                                                   , Tasa_oferta_automatica_maxima_2,
                                                   Pago_oferta_automatica_maxima_2,
                                                   Monto_oferta_automatica_maxima_3,
                                                   No_de_pagos_oferta_automatica_maxima_3,
                                                   Tasa_oferta_automatica_maxima_3
                                                   , Pago_oferta_automatica_maxima_3,
                                                   Monto_oferta_automatica_maxima_4,
                                                   No_de_pagos_oferta_automatica_maxima_4,
                                                   Tasa_oferta_automatica_maxima_4, Pago_oferta_automatica_maxima_4
                                                   , ITA_Tasa_1, ITA_Tasa_2, ITA_Tasa_3, ITA_Tasa_4,
                                                   ITA_No_de_pagos_1, ITA_No_de_pagos_2, ITA_No_de_pagos_3,
                                                   ITA_No_de_pagos_4, NR_1, Flag_Ruta, fecha_envio,
                                                   SAP_estatus_cliente, SAP_subestatus_cliente
                                                   , Estatus_seguimientoCartera, Folio, Etapa_cliente,
                                                   Estatus_cliente, Folio_Movilidad, Subetapa_cliente,
                                                   Pantalla_cliente, Macroproceso_cliente, Flag_prospecto,
                                                   SAP_telefono1, SAP_telefono2, id, ITA_Monto)

    doc_trazabilidad = build_doc_trazabilidad_cliente(Numero_de_Prospecto, Contrato, Etapa_cliente,
                                                      Estatus_cliente, Subetapa_cliente, Macroproceso_cliente,
                                                      Pantalla_cliente, Folio, Folio_Movilidad,
                                                      Estatus_seguimientoCartera
                                                      , SAP_estatus_cliente, SAP_subestatus_cliente,
                                                      Flag_prospecto, id)

    elastic = Elasticsearch() #conexion local


    print(doc_trazabilidad)
    print(doc_renovaciones)

    res_renovaciones = elastic.index(index="renovacionestopup_test", doc_type='prospeccion', body=doc_renovaciones)
    res_trazabilidad = elastic.index(index="trazabilidad_cliente_test", doc_type='renovaciones', body=doc_trazabilidad)

    print("trazabilidad", res_trazabilidad['result'])
    print("renovaciones", res_renovaciones['result'])


elastic_ingest_docs()
