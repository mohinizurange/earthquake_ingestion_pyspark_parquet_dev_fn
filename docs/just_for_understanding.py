# {"type":"FeatureCollection",
#
# "metadata":{"generated":1729441744000,"url":"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
# 			"title":"USGS All Earthquakes, Past Month","status":200,"api":"1.14.1","count":8508},
#
# "features":[
# 			{"type":"Feature","properties":{"mag":1.5,"place":"10 km WSW of Healy, Alaska",
# 			"time":1729440534315,"updated":1729440652706,"tz":null,"url":"https://earthquake.usgs.gov/earthquakes/eventpage/ak024dihksyv",
# 			"detail":"https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/ak024dihksyv.geojson","felt":null,"cdi":null,"mmi":null,"alert":null,
# 			"status":"automatic","tsunami":0,"sig":35,"net":"ak","code":"024dihksyv","ids":",ak024dihksyv,","sources":",ak,","types":",origin,phase-data,",
# 			"nst":null,"dmin":null,"rms":0.29,"gap":null,"magType":"ml","type":"earthquake","title":"M 1.5 - 10 km WSW of Healy, Alaska"},
# 		"geometry":{"type":"Point","coordinates":[-149.1764,63.8362,115.8]},"id":"ak024dihksyv"}
#
# 		{"type":"Feature","properties":{"mag":1.06,"place":"3 km SE of The Geysers, CA","time":1729440376550,"updated":1729441337702,"tz":null,
# 		"url":"https://earthquake.usgs.gov/earthquakes/eventpage/nc75076441","detail":"https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/nc75076441.geojson",
# 		"felt":null,"cdi":null,"mmi":null,"alert":null,"status":"automatic","tsunami":0,"sig":17,"net":"nc","code":"75076441","ids":",nc75076441,","sources":",nc,",
# 		"types":",nearby-cities,origin,phase-data,scitech-link,","nst":14,"dmin":0.002808,"rms":0.04,"gap":72,"magType":"md",
# 		"type":"earthquake","title":"M 1.1 - 3 km SE of The Geysers, CA"},
# 		"geometry":{"type":"Point","coordinates":[-122.727500915527,38.763500213623,2.55999994277954]},"id":"nc75076441"},
#
#
# 		]
# }
#
#
# ############################################################################################################# fetch requied data from above api data #################################
# [
# {'mag': 1.6, 'place': '28 km SW of Garden City, Texas', 'time': 1726852956153, 'updated': 1727101406646, 'tz': None,
#  'url': 'https://earthquake.usgs.gov/earthquakes/eventpage/tx2024snvs', 'detail': 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/tx2024snvs.geojson',
#  'felt': None, 'cdi': None, 'mmi': None, 'alert': None, 'status': 'reviewed', 'tsunami': 0, 'sig': 39, 'net': 'tx', 'code': '2024snvs', 'ids': ',tx2024snvs,',
#  'sources': ',tx,', 'types': ',origin,phase-data,', 'nst': 9, 'dmin': 0.1, 'rms': 0.1, 'gap': 80, 'magType': 'ml', 'type': 'earthquake',
#  'title': 'M 1.6 - 28 km SW of Garden City, Texas',
#  'geometry': [-101.716, 31.707, 9.6924],
#  'id': 'tx2024snvs'},
#  {...................
#  }
#
#  ]
#
#  schema = StructType([
#               StructField("mag",FloatType(),True),
# 			  StructField("place",StringType(),True),
# 			  StructField("time",StringType(),True),
# 			  StructField("updated",StringType(),True),
# 			  StructField("tz",StringType(),True),
#               StructField("url",StringType(),True),
#               StructField("detail",StringType(),True),
# 		      StructField("felt",StringType(),True),
# 			  StructField("cdi",StringType(),True),
# 			  StructField("mmi",StringType(),True),
# 			  StructField("alert",StringType(),True),
# 			  StructField("status",StringType(),True),
# 			  StructField("tsunami",FloatType(),True),
# 		      StructField("sig",IntegerType(),True),
#               StructField("net",StringType(),True),
# 			  StructField("code",StringType(),True),
# 			  StructField("ids",StringType(),True),
# 			  StructField("sources",StringType(),True),
# 		      StructField("types",StringType(),True),
#               StructField("nst",IntegerType(),True),
# 			  StructField("dmin",FloatType(),True),
# 			  StructField("rms",FloatType(),True),
# 			  StructField("gap",IntegerType(),True),
# 		      StructField("magType",StringType(),True)
#                StructField("type",StringType(),True),
# 			  StructField("title",StringType(),True),
# 			  StructField("geometry",ArrayType(IntegerType()),True),
# 		      StructField("id",StringType(),True)
#
#
#           ])