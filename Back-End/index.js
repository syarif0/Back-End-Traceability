const dotenv = require('dotenv');
dotenv.config();
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

// MQTT Configuration
const mqttUsername = process.env.MQTT_USERNAME;
const mqttPassword = process.env.MQTT_PASSWORD;
const mqttHost = process.env.MQTT_HOST;
const mqttPort = process.env.MQTT_PORT || 8883;

// Supabase Configuration
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// MQTT Connection Options
const mqttOptions = {
  username: mqttUsername,
  password: mqttPassword,
  rejectUnauthorized: false,
};

// Construct the MQTT connection URL
const mqttConnectionUrl = `tls://${mqttHost}:${mqttPort}`;

// Create MQTT Client
const client = mqtt.connect(mqttConnectionUrl, mqttOptions);

// Topic Definitions
const topicAbbreviations = {
  'Mixing': 'MIX',
  'Logging': 'LOG',
  'Sterilisasi': 'STR',
  'Inokulasi': 'INO',
  'Inkubasi': 'INK',
  'Kumbung': 'KMB'
};

// MQTT Event Handlers
client.on('connect', () => {
  // Subscribe using full topic
  client.subscribe('Mixing/+', (err) => { 
    if (err) { console.error('Error subscribing to Mixing topics:', err); } 
  });
  client.subscribe('Logging/+', (err) => { 
    if (err) { console.error('Error subscribing to Logging topics:', err); } 
  });
  client.subscribe('Sterilisasi/+', (err) => { 
    if (err) { console.error('Error subscribing to Sterilisasi topics:', err); } 
  });
  client.subscribe('Inokulasi/+', (err) => { 
    if (err) { console.error('Error subscribing to Inokulasi topics:', err); } 
  });
  client.subscribe('Inkubasi/+', (err) => { 
    if (err) { console.error('Error subscribing to Inkubasi topics:', err); } 
  });
  client.subscribe('Kumbung/+/+', (err) => { 
    if (err) { console.error('Error subscribing to Kumbung topics:', err); } 
  });
});

client.on('message', async (topic, message) => {
  try {
    const messageContent = JSON.parse(message.toString());
    const timestamp = new Date().toLocaleString('en-US', { timeZone: 'Asia/Jakarta' });

    const topicParts = topic.split('/');
    const placeCode = topicAbbreviations[topicParts[0]];

    if (!placeCode) {
      console.error("Invalid place name in MQTT topic:", topic);
      return;
    }

    let areaCode = "";
    let locationCode = "";

    if (placeCode === "KMB") {
      areaCode = topicParts[1]; 
      locationCode = topicParts[2];
    } else {
      locationCode = topicParts[1]; 
    }

    let idEsp = `${placeCode}`;
    if (areaCode) { 
      idEsp += `-${areaCode}`; 
    }
    idEsp += `-${locationCode}`;

    const sensorData = {};
    for (const key in messageContent) {
      if (messageContent.hasOwnProperty(key)) {
        sensorData[key] = messageContent[key];
      }
    }

    const { error } = await supabase
      .from('DataSensor')
      .insert([
        {
          ID_ESP: idEsp,
          Data_Sensor: sensorData,
          Timestamp: timestamp,
        },
      ]);

    if (error) {
      console.error('Error inserting data:', error);
    } 

  } catch (error) {
    console.error('Error processing message:', error);
  }
});

client.on('error', (error) => {
  console.error('MQTT Error:', error);
});
