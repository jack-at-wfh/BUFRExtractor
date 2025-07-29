# BUFR Format Overview

The **Binary Universal Form for the Representation of meteorological data (BUFR)** is a binary representation of meteorological data. It is a continuous bit stream made up of a sequence of octets (one octet is eight bits). The only part of BUFR where information does not end on byte boundaries is the data section, where a length of BUFR table B elements can have any number of bits (although it must not exceed the number of bits in a computer word for non-character data).

A BUFR message consists of six sections, some of which may be completely optional (Section 2) or partially optional (Section 1). The representation of data in the form of a series of bits is independent of any particular machine representation. It is important to stress that the BUFR representation is not suitable for data visualization without computer interpretation.

A BUFR message is comprised of the following sections:

* **Indicator section (Section 0)**

* **Identification section (Section 1)**

* **Optional section (Section 2)**

* **Data description section (Section 3)**

* **Data section (Section 4)**

* **End section (Section 5)**

Each section included in the message always contains an even number of octets. If necessary, sections must be appended with bits set to zero to fulfill this requirement.

## SECTION 0 - INDICATOR SECTION

The Indicator section, or Section 0, of a BUFR message has a fixed length of eight octets. Information about the total size of the BUFR message in octets 5-7 is very useful for reading BUFR data from pure binary files.
### Table 1: BUFR Section - 0 (Indicator Section)
| Octet number | Content |
| :----------- | :----- |
| 1-4 | BUFR four letters in CCITT International Alphabet No.5 |
| 5-7 | Total length of BUFR message in bytes |
| 8 | BUFR Edition number (currently 4) |
|
The layout of the Identification section is given in Table 2.

## SECTION 1 - IDENTIFICATION SECTION

This section contains information relevant to data recognition without performing complete expansion of data. Data type and observation date and time are the most important parts of it. In the case of multi-subset data, the time of the earliest observation should be packed into Section 1. This section also contains all information necessary to define the BUFR tables used.
### Table 2: BUFR Section - 1 (Identification Section)
| Octet number | Content |
| :----------- | :------------------------------------------------------------------------- |
| 1-3          | Length of section 1                                                        |
| 4            | BUFR Master Table (zero if standard WMO FM 94-IX BUFR tables are used)     |
| 5-6          | Identification of originating/generating centre                            |
| 7-8          | Identification of originating/generating sub-centre                        |
| 9            | Update sequence number (zero for original BUFR messages; incremented by one for updates) |
| 10           | Bit 1 = 0 No optional section<br>Bit 1 = 1 Optional section follows<br>Bit 2-8 Set to zero (reserved) |
| 11           | Data Category (Table A)                                                    |
| 12           | International data sub-category                                            |
| 13           | Local sub-category                                                         |
| 14           | Version number of master table used                                        |
| 15           | Version number of local tables used to augment the master table in use     |
| 16-17        | Year (4 digits)                                                            |
| 18           | Month                                                                      |
| 19           | Day                                                                        |
| 20           | Hour                                                                       |
| 21           | Minute                                                                     |
| 22           | Second                                                                     |
| 23-          | Reserved for local use by ADP centres                                      |
|
## SECTION 2 - OPTIONAL SECTION (Additional Detail)

The presence of Section 2 of the Bufr message is indicated by a flag in the 8th byte of Section 1. This section can be used locally by Automated Data Processing centres. This Section is used to keep the Report Data Base key.
The layout of Section 2 is given in table 3.

### Table 3: Bufr Section - 2
|Octet |Content |
|:---- |:-----------|
|1-3   |Length of section in bytes |
|4     |Set to zero (reserved) |
|5-    |reserved for local use by ADP centres |
|

## SECTION 3 - DATA DESCRIPTION SECTION
This section describes the data in the data section. The information which can be found in the first seven octets is the number of subsets in the message, their form and the type of data (observation/non-observation). The data descriptors start in the 8th octet of the section 3. Each descriptor is spread over two bytes and contains three parts. If F = 0, the descriptor is an element descriptor and values of X and Y define entries in Bufr Table B.

### Table 4: Descriptor reference
|F |X |Y |
|:----|:-----|:-----|
|2 bits | 6 bits | 8 bits |
|
For F = 1, the descriptor is a replication descriptor. 
If F = 2, the descriptor is one of the operators from Bufr Table C. 
F = 3 means that the descriptor represents the sequence descriptor from Bufr Table D. 
The table D entries contain a list of element descriptors, operators, and/or other sequence descriptors. In an ideal situation, data in Section 4 should be described by one Bufr Table D entry only. X stands for class of elements in the range from 0-63 and Y is an entry within class 0-255. Classes 48-63 are reserved for local use and entries from 192-255 within all classes are also reserved for local usage. Layout of Data description section is given in the Table 5.
### Table 5: Data description section

|Octet |Content |
|:-----|:-------|
|1-3   |Length of section |
|4     |set to zero (reserved) |
|5-6   |Number of data subsets |
|7     |Bit 1 = 1 Observed data |
|      |Bit 1 = 0 Other data |
|      |Bit 2 = 1 Compressed data |
|      |Bit 2 = 0 Non compressed data |
|      |Bits 3-8 set to zero ( reserved) |
|8-    |A collection of element descriptors, replication descriptors, operator descriptors and sequence descriptors, which define the form and contents of individual data elements comprising one data subset in the data section. |
|

## Section 4 - DATA SECTION
The Data section, like all sections, starts with the length of Section 4 followed by a continuous stream of bits from byte 5 onward. Layout of Data section is given in the Table 6.
### Table 6: Data section
|Octet | Content |
|:-----|:------------|
|1-3 |Length of section in bytes |
|4 |set to zero (reserved) |
|5- |Binary data as defined by sequence descriptors |
|
## Section 5 - END SECTION
The End section is comprised of four ”7” characters in CCITT International Alphabet No.5 and this marks the end of the Bufr message. The layout of the End section is given in the Table 7.
### Table 7: End section
|Octet |Content  |
|:-----|:--------|
|1-4 | ”7777” (coded according to the CCITT No 5) |
|