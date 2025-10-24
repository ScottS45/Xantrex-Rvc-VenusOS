## Changelog

### Oct 24, 2025
- Fixed current decoding logic  
  - Updated helper from safe_s16 → safe_u8 for current values in DGNs 0x1FEE8 (Inverter DC Status) and 0x1FFCA (Charger AC Status 1).  
  - Adjusted byte offsets and scaling factors to correctly reflect the actual current readings.  
  - Special thanks to Stephen for extensive testing, validation, and analysis ideas.  
- Added missing D-Bus path registrations  
  - /Ac/Grid/P  
  - /Ac/Grid/I  

---

### Oct 18, 2025
- Syntax fixes  
  - Corrected minor syntax issues (missing ':' and extra '=') in firmware retrieval logic.  
  - Thanks to Stephen Tenberg for spotting and verifying the fixes.
