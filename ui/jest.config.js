module.exports = {
  setupFilesAfterEnv: ['./jestSetup.ts'],
  displayName: 'test',
  testURL: 'http://localhost',
  testPathIgnorePatterns: [
    '<rootDir>/build',
    '<rootDir>/node_modules/(?!(jest-test))',
    '<rootDir>/cypress',
  ],
  setupFiles: ['<rootDir>/testSetup.ts'],
  modulePaths: ['<rootDir>', '<rootDir>/node_modules'],
  moduleDirectories: ['src'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  // https://github.com/facebook/jest/issues/7842
  testMatch: '<rootDir>/**/?(*.)+(spec|test).[tj]s?(x)',
  //testRegex: '<rootDir>/**/*\.test\.{tsx,ts}$',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  snapshotSerializers: ['enzyme-to-json/serializer'],
  moduleNameMapper: {
    '\\.(css|scss)$': 'identity-obj-proxy',
  },
  globals: {
    'ts-jest': {
      tsConfig: 'tsconfig.test.json',
    },
  },
  collectCoverageFrom: [
    './src/**/*.{js,jsx,ts,tsx}',
    '!./src/**/*.test.{js,jsx,ts,tsx}',
  ],
  coverageDirectory: './coverage',
  coverageReporters: ['html', 'cobertura'],
}
