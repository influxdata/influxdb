# Windows Eventlog Input Plugin

## Collect Windows Event Log messages

Supports Windows Vista and higher.

Telegraf should have Administrator permissions to subscribe for some of the Windows Events Channels, like System Log.

Telegraf minimum version: Telegraf 1.16.0

### Configuration

```toml
[[inputs.win_eventlog]]
  ## Telegraf should have Administrator permissions to subscribe for some Windows Events channels
  ## (System log, for example)

  ## LCID (Locale ID) for event rendering
  ## 1033 to force English language
  ## 0 to use default Windows locale
  # locale = 0

  ## Name of eventlog, used only if xpath_query is empty
  ## Example: "Application"
  # eventlog_name = ""

  ## xpath_query can be in defined short form like "Event/System[EventID=999]"
  ## or you can form a XML Query. Refer to the Consuming Events article:
  ## https://docs.microsoft.com/en-us/windows/win32/wes/consuming-events
  ## XML query is the recommended form, because it is most flexible
  ## You can create or debug XML Query by creating Custom View in Windows Event Viewer
  ## and then copying resulting XML here
  xpath_query = '''
  <QueryList>
    <Query Id="0" Path="Security">
      <Select Path="Security">*</Select>
      <Suppress Path="Security">*[System[( (EventID &gt;= 5152 and EventID &lt;= 5158) or EventID=5379 or EventID=4672)]]</Suppress>
    </Query>
    <Query Id="1" Path="Application">
      <Select Path="Application">*[System[(Level &lt; 4)]]</Select>
    </Query>
    <Query Id="2" Path="Windows PowerShell">
      <Select Path="Windows PowerShell">*[System[(Level &lt; 4)]]</Select>
    </Query>
    <Query Id="3" Path="System">
      <Select Path="System">*</Select>
    </Query>
    <Query Id="4" Path="Setup">
      <Select Path="Setup">*</Select>
    </Query>
  </QueryList>
  '''

  ## System field names:
  ##   "Source", "EventID", "Version", "Level", "Task", "Opcode", "Keywords", "TimeCreated",
  ##   "EventRecordID", "ActivityID", "RelatedActivityID", "ProcessID", "ThreadID", "ProcessName",
  ##   "Channel", "Computer", "UserID", "UserName", "Message", "LevelText", "TaskText", "OpcodeText"

  ## In addition to System, Data fields can be unrolled from additional XML nodes in event.
  ## Human-readable representation of those nodes is formatted into event Message field,
  ## but XML is more machine-parsable

  # Process UserData XML to fields, if this node exists in Event XML
  process_userdata = true

  # Process EventData XML to fields, if this node exists in Event XML
  process_eventdata = true

  ## Separator character to use for unrolled XML Data field names
  separator = "_"

  ## Get only first line of Message field. For most events first line is usually more than enough
  only_first_line_of_message = true

  ## Fields to include as tags. Globbing supported ("Level*" for both "Level" and "LevelText")
  event_tags = ["Source", "EventID", "Level", "LevelText", "Task", "TaskText", "Opcode", "OpcodeText", "Keywords", "Channel", "Computer"]

  ## Default list of fields to send. All fields are sent by default. Globbing supported
  event_fields = ["*"]

  ## Fields to exclude. Also applied to data fields. Globbing supported
  exclude_fields = ["Binary", "Data_Address*"]

  ## Skip those tags or fields if their value is empty or equals to zero. Globbing supported
  exclude_empty = ["*ActivityID", "UserID"]
```

### Filtering

There are three types of filtering: **Event Log** name, **XPath Query** and **XML Query**.

**Event Log** name filtering is simple:

```toml
  eventlog_name = "Application"
  xpath_query = '''
```

For **XPath Query** filtering set the `xpath_query` value, and `eventlog_name` will be ignored:

```toml
  eventlog_name = ""
  xpath_query = "Event/System[EventID=999]"
```

**XML Query** is the most flexible: you can Select or Suppress any values, and give ranges for other values. XML query is the recommended form, because it is most flexible. You can create or debug XML Query by creating Custom View in Windows Event Viewer and then copying resulting XML in config file.

XML Query documentation:

<https://docs.microsoft.com/en-us/windows/win32/wes/consuming-events>

### Metrics

You can send any field, *System*, *Computed* or *XML* as tag field. List of those fields is in the `event_tags` config array. Globbing is supported in this array, i.e. `Level*` for all fields beginning with `Level`, or `L?vel` for all fields where the name is `Level`, `L3vel`, `L@vel` and so on. Tag fields are converted to strings automatically.

By default, all other fields are sent, but you can limit that either by listing it in `event_fields` config array with globbing, or by adding some field name masks in the `exclude_fields` config array.

You can limit sending fields with empty values by adding masks of names of such fields in the `exclude_empty` config array. Value considered empty, if the System field of type `int` or `uint32` is equal to zero, or if any field of type `string` is an empty string.

List of System fields:

- Source (string)
- EventID (int)
- Version (int)
- Level (int)
- LevelText (string)
- Opcode (int)
- OpcodeText (string)
- Task (int)
- TaskText (string)
- Keywords (string): comma-separated in case of multiple values
- TimeCreated (string)
- EventRecordID (string)
- ActivityID (string)
- RelatedActivityID (string)
- ProcessID (int)
- ThreadID (int)
- ProcessName (string): derived from ProcessID
- Channel (string)
- Computer (string): useful if consumed from Forwarded Events
- UserID (string): SID
- UserName (string): derived from UserID, presented in form of DOMAIN\Username
- Message (string)

### Computed fields

Fields `Level`, `Opcode` and `Task` are converted to text and saved as computed `*Text` fields.

`Keywords` field is converted from hex uint64 value by the `_EvtFormatMessage` WINAPI function. There can be more than one value, in that case they will be comma-separated. If keywords can't be converted (bad device driver or forwarded from another computer with unknown Event Channel), hex uint64 is saved as is.

`ProcessName` field is found by looking up ProcessID. Can be empty if telegraf doesn't have enough permissions.

`Username` field is found by looking up SID from UserID.

`Message` field is rendered from the event data, and can be several kilobytes of text with line breaks. For most events the first line of this text is more then enough, and additional info is more useful to be parsed as XML fields. So, for brevity, plugin takes only the first line. You can set `only_first_line_of_message` parameter to `false` to take full message text.

### Additional Fields

The content of **Event Data** and **User Data** XML Nodes can be added as additional fields, and is added by default. You can disable that by setting `process_userdata` or `process_eventdata` parameters to `false`.

For the fields from additional XML Nodes the `Name` attribute is taken as the name, and inner text is the value. Type of those fields is always string.

Name of the field is formed from XML Path by adding _ inbetween levels. For example, if UserData XML looks like this:

```xml
<UserData>
 <CbsPackageChangeState xmlns="http://manifests.microsoft.com/win/2004/08/windows/setup_provider">
  <PackageIdentifier>KB4566782</PackageIdentifier>
  <IntendedPackageState>5112</IntendedPackageState>
  <IntendedPackageStateTextized>Installed</IntendedPackageStateTextized>
  <ErrorCode>0x0</ErrorCode>
  <Client>UpdateAgentLCU</Client>
 </CbsPackageChangeState>
</UserData>
```

It will be converted to following fields:

```text
CbsPackageChangeState_PackageIdentifier = "KB4566782"
CbsPackageChangeState_IntendedPackageState = "5112"
CbsPackageChangeState_IntendedPackageStateTextized = "Installed"
CbsPackageChangeState_ErrorCode = "0x0"
CbsPackageChangeState_Client = "UpdateAgentLCU"
```

If there are more than one field with the same name, all those fields are given suffix with number: `_1`, `_2` and so on.

### Localization

Human readable Event Description is in the Message field. But it is better to be skipped in favour of the Event XML values, because they are more machine-readable.

Keywords, LevelText, TaskText, OpcodeText and Message are saved with the current Windows locale by default. You can override this, for example, to English locale by setting `locale` config parameter to `1033`. Unfortunately, **Event Data** and **User Data** XML Nodes are in default Windows locale only.

Locale should be present on the computer. English locale is usually available on all localized versions of modern Windows. List of locales:

<https://docs.microsoft.com/en-us/openspecs/office_standards/ms-oe376/6c085406-a698-4e12-9d4d-c3b0ee3dbc4a>

### Example Output

Some values are changed for anonymity.

```text
win_eventlog,Channel=System,Computer=PC,EventID=105,Keywords=0x8000000000000000,Level=4,LevelText=Information,Opcode=10,OpcodeText=General,Source=WudfUsbccidDriver,Task=1,TaskText=Driver,host=PC ProcessName="WUDFHost.exe",UserName="NT AUTHORITY\\LOCAL SERVICE",Data_dwMaxCCIDMessageLength="271",Data_bPINSupport="0x0",Data_bMaxCCIDBusySlots="1",EventRecordID=1914688i,UserID="S-1-5-19",Version=0i,Data_bClassGetEnvelope="0x0",Data_wLcdLayout="0x0",Data_bClassGetResponse="0x0",TimeCreated="2020-08-21T08:43:26.7481077Z",Message="The Smartcard reader reported the following class descriptor (part 2)." 1597999410000000000

win_eventlog,Channel=Security,Computer=PC,EventID=4798,Keywords=Audit\ Success,Level=0,LevelText=Information,Opcode=0,OpcodeText=Info,Source=Microsoft-Windows-Security-Auditing,Task=13824,TaskText=User\ Account\ Management,host=PC Data_TargetDomainName="PC",Data_SubjectUserName="User",Data_CallerProcessId="0x3d5c",Data_SubjectLogonId="0x46d14f8d",Version=0i,EventRecordID=223157i,Message="A user's local group membership was enumerated.",Data_TargetUserName="User",Data_TargetSid="S-1-5-21-.-.-.-1001",Data_SubjectUserSid="S-1-5-21-.-.-.-1001",Data_CallerProcessName="C:\\Windows\\explorer.exe",ActivityID="{0d4cc11d-7099-0002-4dc1-4c0d9970d601}",UserID="",Data_SubjectDomainName="PC",TimeCreated="2020-08-21T08:43:27.3036771Z",ProcessName="lsass.exe" 1597999410000000000

win_eventlog,Channel=Microsoft-Windows-Dhcp-Client/Admin,Computer=PC,EventID=1002,Keywords=0x4000000000000001,Level=2,LevelText=Error,Opcode=76,OpcodeText=IpLeaseDenied,Source=Microsoft-Windows-Dhcp-Client,Task=3,TaskText=Address\ Configuration\ State\ Event,host=PC Version=0i,Message="The IP address lease 10.20.30.40 for the Network Card with network address 0xaabbccddeeff has been denied by the DHCP server 10.20.30.1 (The DHCP Server sent a DHCPNACK message).",UserID="S-1-5-19",Data_HWLength="6",Data_HWAddress="545595B7EA01",TimeCreated="2020-08-21T08:43:42.8265853Z",EventRecordID=34i,ProcessName="svchost.exe",UserName="NT AUTHORITY\\LOCAL SERVICE" 1597999430000000000

win_eventlog,Channel=System,Computer=PC,EventID=10016,Keywords=Classic,Level=3,LevelText=Warning,Opcode=0,OpcodeText=Info,Source=Microsoft-Windows-DistributedCOM,Task=0,host=PC Data_param3="Активация",Data_param6="PC",Data_param8="S-1-5-21-2007059868-50816014-3139024325-1001",Version=0i,UserName="PC\\User",Data_param1="по умолчанию для компьютера",Data_param2="Локально",Data_param7="User",Data_param9="LocalHost (с использованием LRPC)",Data_param10="Microsoft.Windows.ShellExperienceHost_10.0.19041.423_neutral_neutral_cw5n1h2txyewy",ActivityID="{839cac9e-73a1-4559-a847-62f3a5e73e44}",ProcessName="svchost.exe",Message="The по умолчанию для компьютера permission settings do not grant Локально Активация permission for the COM Server application with CLSID ",Data_param5="{316CDED5-E4AE-4B15-9113-7055D84DCC97}",Data_param11="S-1-15-2-.-.-.-.-.-.-2861478708",TimeCreated="2020-08-21T08:43:45.5233759Z",EventRecordID=1914689i,UserID="S-1-5-21-.-.-.-1001",Data_param4="{C2F03A33-21F5-47FA-B4BB-156362A2F239}" 1597999430000000000

```
