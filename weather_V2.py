#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
   NeOCampus mqtt weather formatter
   Takes weather data from the Toulouse Metropole weather station from weewx via MQTT
   Performs an average of the received values over a given interval
   Republishes the data into a defined MQTT topic
   Author : Sebastian Lucas 2019-2020
"""

############
#Imports
import paho.mqtt.client as mqtt #import the mqtt client
import time #Used for the timing
import json #Used for converting to and from json strings
#import re
import math
import scipy #Used for wind average
import numpy as np #Used for converting list to array
############

##########################
##----USER SETTINGS----###
##########################

#Update time interval for published average data (in seconds) - This interval isn't precise as weewx sends data every minute, data should update within a minute of defined interval
definedInterval = 10 #Seconds

#Connection variables, change as required
MQTT_server = "neocampus.univ-tlse3.fr"
MQTT_user = "test"
MQTT_password = "test"
#The MQTT topic where we find the weewx default loop topic: example: TestTopic/_meteo
MQTT_topic = "TestTopic/_meteo"

#The MQTT topic to publish outdoor data into (weather station data)
MQTT_outdoor_publish_topic = "TestTopic/_meteo/outside/IRIT"

#The MQTT topic to publish indoor data into (receiver unit data)
MQTT_indoor_publish_topic = "TestTopic/_meteo/inside/IRIT"

##########################
##----END  SETTINGS----###
##########################

#Set default values for the global variables

curInTemp = []
curOutTemp = []
curInHumidity = []
curOutHumidity = []
curWindDir = []
curWindSpeed = []
curHourRain = []
curPressure = []

#Get the current time
start_time = time.time()

#Function to calculate average wind speed and direction (input of type array): https://github.com/Kirubaharan/hydrology/blob/master/checkdam/meteolib.py
def windvec(u,D):

    ve = 0.0 # define east component of wind speed
    vn = 0.0 # define north component of wind speed
    D = D * math.pi / 180.0 # convert wind direction degrees to radians

    for i in range(0, len(u)):
        ve = ve + u[i] * math.sin(D[i]) # calculate sum east speed components
        vn = vn + u[i] * math.cos(D[i]) # calculate sum north speed components

    ve = - ve / len(u) # determine average east speed component
    vn = - vn / len(u) # determine average north speed component

    uv = math.sqrt(ve * ve + vn * vn) # calculate wind speed vector magnitude
    # Calculate wind speed vector direction
    vdir = scipy.arctan2(ve, vn)
    vdir = vdir * 180.0 / math.pi # Convert radians to degrees

    if vdir < 180:
        Dv = vdir + 180.0
    else:
        if vdir > 180.0:
            Dv = vdir - 180
        else:
            Dv = vdir

    return uv, Dv # uv in m/s, Dv in degrees from North

#Function to get the average of a list
def Average(lst): 
    return sum(lst) / len(lst)

#Outputs log messages and call-backs in the console
def on_log(mqttc, obj, level, string):
    print(string)

#Code to execute when any MQTT message is received
def on_message(client, userdata, message):


    #We specify that the variables are GLOBAL
    global curInTemp
    global curOutTemp
    global curInHumidity
    global curOutHumidity
    global curWindDir
    global curWindSpeed
    global curHourRain
    global curPressure
    global start_time

    #Display the received message in the console
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)

    #Check if the message topic is "loop" and decode the weather data
    if(message.topic == MQTT_topic + "/loop"):
           #We convert the loop JSON string to a python dictionary
           inData = json.loads(str(message.payload.decode("utf-8")))
           print("We loaded the JSON data!")
           #We load the data from the dictionary using the keys

           #Outdoor data
           curPressure.append(float(inData["pressure_mbar"]))
           curOutHumidity.append(float(inData["outHumidity"]))
           curWindSpeed.append(float(inData["windSpeed_kph"])/3.6) #Wind Speed in m/s
           curOutTemp.append(float(inData["outTemp_C"]))
           curWindDir.append(float(inData["windDir"]))
           curHourRain.append(float(inData["hourRain_cm"]))

           #Indoor data
           curInTemp.append(float(inData["inTemp_C"]))
           curInHumidity.append(float(inData["inHumidity"]))

           print("Appended all data")

    #We check if we have reached the defined interval yet
    if(time.time() > start_time+definedInterval):

            #Average wind direction
            windAvg = windvec(np.asarray(curWindSpeed), np.asarray(curWindDir))

            #We set our outdoor output string, performing an average on all value lists (note that windSpeed is multiplied by 3.6 to convert back to kph)
            outData_outside = {"pressure_mbar": Average(curPressure), "outHumidity": Average(curOutHumidity), "windSpeed_kph": 3.6*windAvg[0], "outTemp_C": Average(curOutTemp), "windDir": windAvg[1], "hourRain_cm": Average(curHourRain)}
            #We set out indoor output string
            outData_inside = {"inHumidity": Average(curInHumidity), "inTemp_C": Average(curInTemp)}

            #We publish the output string converting it to JSON first
            print("Publishing message to topic", MQTT_outdoor_publish_topic)
            client.publish(MQTT_outdoor_publish_topic, json.dumps(outData_outside))
            print("Publishing message to topic", MQTT_indoor_publish_topic)
            client.publish(MQTT_indoor_publish_topic, json.dumps(outData_inside))

            #We clear all previous values and reset the time
            curPressure.clear()
            curOutHumidity.clear()
            curWindSpeed.clear()
            curOutTemp.clear()
            curWindDir.clear()
            curHourRain.clear()
            curInTemp.clear()
            curInHumidity.clear()
            start_time = time.time()

#Start of the MQTT subscribing
########################################

#MQTT address
broker_address=MQTT_server
print("creating new instance")
client = mqtt.Client("P1") #create new instance
client.on_message=on_message #attach function to callback
client.on_log=on_log #attach logging to log callback

# Auth
client.username_pw_set(username=MQTT_user,password=MQTT_password)

# now we connect
print("connecting to broker")
client.connect(broker_address) #connect to broker

#Subscribe to all the weather topics we need
print("Subscribing to topic",MQTT_topic + "/loop")
client.subscribe(MQTT_topic + "/loop")

#Tell the MQTT client to subscribe forever
client.loop_forever()
