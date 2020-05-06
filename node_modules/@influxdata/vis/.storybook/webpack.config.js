module.exports = ({config}) => {
  config.module.rules.push({
    test: /\.(ts|tsx)$/,
    use: [
      {
        loader: require.resolve('ts-loader'),
        options: {
          compilerOptions: {
            rootDir: null,
            outDir: null,
            declaration: false,
          },
        },
      },
    ],
  })
  config.resolve.extensions.push('.ts', '.tsx')
  return config
}
