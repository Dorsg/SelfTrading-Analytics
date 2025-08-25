import { themeQuartz } from 'ag-grid-community';


export const customDarkTheme = themeQuartz.withParams({
  backgroundColor: '#0E0E10',                // Almost black (main background)
  foregroundColor: '#E0E0E0',                // Soft light gray for text
  headerBackgroundColor: '#16161A',          // Very dark grey-black for header
  headerTextColor: '#F5F5F5',                // Brighter white for header labels
  oddRowBackgroundColor: '#121214',          // Slightly lifted for stripe contrast
  rowHoverColor: '#1E1E22',                  // Subtle hover effect
  selectedRowBackgroundColor: '#2A2A2F',     // Still dark but visibly selected
  borderColor: '#2C2C30',                    // Soft edges and outlines
  accentColor: '#3D7FFF',                    // Cool electric blue highlight
});


export const runnerTheme = themeQuartz.withParams({
  backgroundColor: '#1A1D24',               // Soft blue-gray dark base
  foregroundColor: '#E8ECF4',               // Crisp light gray-blue text
  headerBackgroundColor: '#2B313C',         // Mid-tone bluish gray for header
  headerTextColor: '#FFFFFF',               // Pure white for contrast
  oddRowBackgroundColor: '#212631',         // Slight contrast for stripe effect
  rowHoverColor: '#2F3745',                 // Lighter blue-gray hover
  selectedRowBackgroundColor: '#3C4A60',    // Noticeably blue-gray selected row
  borderColor: '#3C4350',                   // Soft blue-gray borders
  accentColor: '#5A9BFF',                   // Vibrant blue for highlights
});