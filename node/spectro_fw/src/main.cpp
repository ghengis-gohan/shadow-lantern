#include <Arduino.h>
/*
  spectro_fw — Hamamatsu C12880MA micro-spectrometer front-end on an Arduino Uno.

  Why an Uno and not the Jetson's GPIO: the sensor is a shift register in disguise. It needs a clean
  clock, a start pulse of a precise length (that IS the integration time) and 288 ADC reads in lockstep
  with the clock. A Linux process over USB can't hold that timing; an 16 MHz MCU does it trivially.

  Wiring (GroupGets C12880MA breakout v2 -> Uno):
    VCC   -> 5V            GND -> GND
    CLK   -> D4            ST  -> D5
    VIDEO -> A0            EOS -> D6 (read only; not required)   TRG -> D7 (unused)

  Wire format to the Jetson, one text line per frame at 115200 baud:
    S,<seq>,<integ_us>,<v0>,<v1>,...,<v287>\n      normal exposure, 288 ten-bit ADC values
    D,<seq>,<integ_us>,<v0>,...                     dark exposure requested by the host (slit covered)
  Commands from the host (single line):
    I<microseconds>\n   set integration time, 1000..1000000 (default 20000)
    D\n                 tag the next frame as dark
    ?\n                 print settings

  Timing follows the GroupGets reference sketch: ST high = integration, then the fixed clock counts the
  datasheet specifies before pixel 0 appears on VIDEO. Frame rate is ADC-bound: ~288 x 112 us plus
  serial output of ~1.5 kB at 115200 (~130 ms) -> roughly 5 frames/s at 20 ms integration.
*/

const uint8_t SPEC_CLK = 4, SPEC_ST = 5, SPEC_EOS = 6, SPEC_VIDEO = A0;
const uint16_t N_PX = 288;

uint16_t px[N_PX];
uint32_t seq = 0;
uint32_t integ_us = 20000;
bool darkNext = false;
char cmd[16]; uint8_t cmdLen = 0;

inline void clk() {
  digitalWrite(SPEC_CLK, HIGH); delayMicroseconds(1);
  digitalWrite(SPEC_CLK, LOW);  delayMicroseconds(1);
}

void readSpectrum() {
  // Start pulse: ST low briefly, then high for the integration period while the clock runs.
  digitalWrite(SPEC_ST, LOW);
  for (uint8_t i = 0; i < 4; i++) clk();
  digitalWrite(SPEC_ST, HIGH);
  uint32_t t0 = micros();
  while ((uint32_t)(micros() - t0) < integ_us) clk();
  digitalWrite(SPEC_ST, LOW);

  // After ST falls, the sensor needs a fixed number of clocks before pixel 0 is valid.
  for (uint8_t i = 0; i < 87; i++) clk();

  // One ADC read per clock for 288 pixels.
  for (uint16_t i = 0; i < N_PX; i++) {
    px[i] = analogRead(SPEC_VIDEO);
    clk();
  }

  // Flush the remaining clocks so the device returns to idle cleanly.
  for (uint8_t i = 0; i < 8; i++) clk();
}

void handleCommand() {
  cmd[cmdLen] = 0;
  if (cmd[0] == 'I') {
    long v = atol(cmd + 1);
    if (v >= 1000 && v <= 1000000L) integ_us = (uint32_t)v;
    Serial.print(F("# integ_us=")); Serial.println(integ_us);
  } else if (cmd[0] == 'D') {
    darkNext = true;
  } else if (cmd[0] == '?') {
    Serial.print(F("# spectro_fw C12880MA integ_us=")); Serial.print(integ_us);
    Serial.print(F(" seq=")); Serial.println(seq);
  }
  cmdLen = 0;
}

void setup() {
  pinMode(SPEC_CLK, OUTPUT); pinMode(SPEC_ST, OUTPUT); pinMode(SPEC_EOS, INPUT);
  digitalWrite(SPEC_CLK, LOW); digitalWrite(SPEC_ST, LOW);
  analogReference(DEFAULT);          // 5 V full scale; VIDEO swings ~0.3..4.5 V
  Serial.begin(115200);
  Serial.println(F("# spectro_fw ready"));
}

void loop() {
  while (Serial.available()) {
    char c = Serial.read();
    if (c == '\n' || c == '\r') { if (cmdLen) handleCommand(); }
    else if (cmdLen < sizeof(cmd) - 1) cmd[cmdLen++] = c;
  }

  readSpectrum();

  Serial.print(darkNext ? 'D' : 'S');
  Serial.print(','); Serial.print(seq++);
  Serial.print(','); Serial.print(integ_us);
  for (uint16_t i = 0; i < N_PX; i++) { Serial.print(','); Serial.print(px[i]); }
  Serial.println();
  darkNext = false;
}
