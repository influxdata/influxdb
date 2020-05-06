[ -f .env ] && source .env

# See https://docs.chromaticqa.com/setup_ci
if [ "${CIRCLE_BRANCH}" != "master" ];
then
  # Chromatic expects to be run via a package.json script rather than through the node bin directly
  npm run chromatic
else \
  npm run chromatic -- --auto-accept-changes
fi
