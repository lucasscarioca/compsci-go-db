/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    'internal/views/**/*.html',
    'assets/static/js/**/*.js'
  ],
  darkMode: 'class',
  theme: {
  },
  plugins: [],
  corePlugins: {
    preflight: true,
  }
}
