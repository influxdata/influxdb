module.exports = {
  projects: [
    {
      displayName: 'test',
      testURL: 'http://localhost',
      testPathIgnorePatterns: [
        'build',
        '<rootDir>/node_modules/(?!(jest-test))',
      ],
      modulePaths: ['<rootDir>', '<rootDir>/node_modules/'],
      moduleDirectories: ['src'],
      setupFiles: ['<rootDir>/testSetup.js'],
      transform: {
        '^.+\\.tsx?$': 'ts-jest',
        '^.+\\.js$': 'babel-jest',
      },
      testRegex: '(/__tests__/.*|(\\.|/)(test))\\.(jsx?|tsx?)$',
      moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
      snapshotSerializers: ['enzyme-to-json/serializer'],
      moduleNameMapper: {
        '\\.(css|scss)$': 'identity-obj-proxy',
      },
    },
    {
      runner: 'jest-runner-eslint',
      displayName: 'eslint',
      testMatch: ['<rootDir>/**/*.test.js'],
    },
  ],
}
