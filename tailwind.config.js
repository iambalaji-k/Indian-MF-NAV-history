/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html"],
  darkMode: "class",
  theme: {
    extend: {
      fontFamily: {
        sans: ["Instrument Sans", "sans-serif"],
        mono: ["Spline Sans Mono", "monospace"],
      },
      colors: {
        ink: {
          DEFAULT: "#141210",
          deep: "#100E0C",
          raise: "#1C1915",
          high: "#26221B",
          line: "#2C2820",
          linestrong: "#3E372C",
        },
        paper: {
          DEFAULT: "#EDE8DC",
          dim: "#A89F8F",
          mute: "#776F61",
        },
        brass: {
          DEFAULT: "#E3A857",
          bright: "#F2BE6E",
        },
        money: {
          DEFAULT: "#5BC48F",
        },
        alert: {
          DEFAULT: "#E5695E",
        },
      },
    },
  },
  plugins: [],
};
