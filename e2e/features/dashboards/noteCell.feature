@feature-dashboards
@dashboards-noteCell
Feature: Dashboards - Dashboard - Note Cell
  As a user I want to Add a Note Cell
  So that I can provide specific static information

# Show note when query return no data makes little sense
# Uses Code Mirror still

  Scenario: Create basic dashboard for notes
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "Dashboards"
#    When hover over the "Dashboards" menu item
#    When click nav sub menu "Dashboards"
    Then the Dashboards page is loaded
    When click the empty Create dashboard dropdown button
    When click the create dashboard item "New Dashboard"
    Then the new dashboard page is loaded
    Then the empty dashboard contains a documentation link
    Then the empty dashboard contains Add a Cell button
    When name dashboard "Rumcajs"

  #N.B. expecting the toggle "show when query returns no data" to not be present
  # TODO #17412 - add check that toggle not present to detect any regression
  Scenario: Exercise Add Note popup controls
    When click dashboard add note button
    # N.B. add guide link check
    Then main "Add" note popup is loaded
    #Then the query no data toggle is not present
    Then dismiss the popup
    Then popup is not loaded
    When click dashboard add note button
    Then click popup cancel simple button
    Then popup is not loaded

  Scenario: Add Note with markdown
    When click dashboard add note button
    When enter the cell note popup CodeMirror text:
  """
  # Večerníček
  ## Rumcajs

   **Loupežník Rumcajs** je _pohádková postava_ z [Večerníčku](https://cs.wikipedia.org/wiki/Ve%C4%8Dern%C3%AD%C4%8Dek), jejíž příběhy následně vyšly i knižně.

   > Jak Rumcajs vysadil duhu na nebe

   * Rumcajs
   * Manka
   * Cipísek

   1. Alpha
   1. Beta
   1. Gamma

  """
    Then the main note popup markdown preview panel contains a "h1" tag with "Večerníček"
    Then the main note popup markdown preview panel contains a "h2" tag with "Rumcajs"
    Then the main note popup markdown preview panel contains a "strong" tag with "Loupežník Rumcajs"
    Then the main note popup markdown preview panel contains a "em" tag with "pohádková postava"
    Then the main note popup markdown preview panel contains a "blockquote" tag with "Jak Rumcajs vysadil duhu na nebe"
    Then the main note popup markdown preview panel contains a "ul li" tag with "Rumcajs"
    Then the main note popup markdown preview panel contains a "ol li" tag with "Alpha"
    When click the cell note popup save button
    Then popup is not loaded
    Then the note cell contains a "h1" tag with "Večerníček"
    Then the note cell contains a "h2" tag with "Rumcajs"
    Then the note cell contains a "strong" tag with "Loupežník Rumcajs"
    Then the note cell contains a "em" tag with "pohádková postava"
    Then the note cell contains a "blockquote" tag with "Jak Rumcajs vysadil duhu na nebe"
    Then the note cell contains a "ul li" tag with "Rumcajs"
    Then the note cell contains a "ol li" tag with "Alpha"

@tested
  Scenario: Edit note
    When toggle context menu of dashboard cell named "Note"
    When click cell content popover edit note
    Then main "Edit" note popup is loaded
    # TODO edit text and verify - need to push on to higher priority tests
    When click popup cancel simple button

  Scenario: Delete note
    When toggle context menu of dashboard cell named "Note"
    When click cell content popover delete
    When click cell content popover delet confirm
    Then the cell named "Note" is no longer present



