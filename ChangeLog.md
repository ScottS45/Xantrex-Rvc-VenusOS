## Changelog

### Oct 27, 2025
 #### *** NOTE: The previous safe_u8 fix for current (I), will not handle higher amps.  u8 is only reading one byte, 8 bits.  That gives a numerical range of 0–255.  The raw max integer is 0xFF = 255.  Multiply that by the scale of 0.05 A/bit	≈ 12.75 A.  ***


### Oct 25, 2025
- Improved the setting of the inverter /State.
  - Commented out the manually calculating, and switched back to FFD4, after finding was off a byte. 
  - Still use some code to better show stand by as it seem to show invertering even when not.
- Switched more decoders to u8 vs u16 
- Still left the set_state function as it helps show off, when the unit is turned off

### Oct 24, 2025
- Fixed current decoding logic  
  - Updated helper from safe_s16 → safe_u8 for current values in DGNs 0x1FEE8 (Inverter DC Status) and 0x1FFCA (Charger AC Status 1).  
  - Adjusted byte offsets and scaling factors to correctly reflect the actual current readings.  
  - Special thanks to Stephen for extensive testing, validation, and analysis ideas.  
- Added missing D-Bus path registrations  
  - /Ac/Grid/P  
  - /Ac/Grid/I  

### Oct 18, 2025
- Syntax fixes  
  - Corrected minor syntax issues (missing ':' and extra '=') in firmware retrieval logic.  
  - Thanks to Stephen Tenberg for spotting and verifying the fixes.
