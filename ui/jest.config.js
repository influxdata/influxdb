module.exports = {
  setupFilesAfterEnv: ['./jestSetup.ts'],
  displayName: 'test',
  testURL: 'http://localhost',
  testPathIgnorePatterns: [
    '<rootDir>/build',
    '<rootDir>/node_modules/(?!(jest-test))',
    'cypress',
  ],
  setupFiles: ['<rootDir>/testSetup.ts'],
  modulePaths: ['<rootDir>', '<rootDir>/node_modules'],
  moduleDirectories: ['src'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  testRegex: '(/__tests__/.*|(\\.|/)(test))\\.(ts?|tsx?)$',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  snapshotSerializers: ['enzyme-to-json/serializer'],
  moduleNameMapper: {
    '\\.(css|scss)$': 'identity-obj-proxy',
  },
  globals: {
    'ts-jest': {
      tsConfig: 'tsconfig.test.json',
      diagnostics: {
        ignoreCodes: [6133, 6192], // ignore unused variable errors
      },
    },
  },
  collectCoverageFrom: [
    './src/**/*.{js,jsx,ts,tsx}',
    '!./src/**/*.test.{js,jsx,ts,tsx}',
  ],
  coverageDirectory: './coverage',
  coverageReporters: ['html', 'cobertura'],
  reporters: ['default', 'jest-junit'],
}
