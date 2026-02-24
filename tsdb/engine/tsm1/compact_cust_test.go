package tsm1_test

import (
	"sync/atomic"
	"testing"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var p1Shard6156Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001344-000000003.tsm", Size: 1499841351}},
	{FileStat: tsm1.FileStat{Path: "000001376-000000003.tsm", Size: 1454446534}},
	{FileStat: tsm1.FileStat{Path: "000001408-000000003.tsm", Size: 1399607073}},
	{FileStat: tsm1.FileStat{Path: "000001416-000000002.tsm", Size: 421254824}},
	{FileStat: tsm1.FileStat{Path: "000001424-000000002.tsm", Size: 380765096}},
	{FileStat: tsm1.FileStat{Path: "000001432-000000002.tsm", Size: 387820890}},
	{FileStat: tsm1.FileStat{Path: "000001433-000000001.tsm", Size: 68184136}},
	{FileStat: tsm1.FileStat{Path: "000001434-000000001.tsm", Size: 94292424}},
	{FileStat: tsm1.FileStat{Path: "000001435-000000001.tsm", Size: 96662382}},
	{FileStat: tsm1.FileStat{Path: "000001436-000000001.tsm", Size: 69906450}},
	{FileStat: tsm1.FileStat{Path: "000001437-000000001.tsm", Size: 51005109}},
	{FileStat: tsm1.FileStat{Path: "000001438-000000001.tsm", Size: 60261346}},
	{FileStat: tsm1.FileStat{Path: "000001439-000000001.tsm", Size: 82568249}},
	{FileStat: tsm1.FileStat{Path: "000005010-000000027.tsm", Size: 2147541391}},
}

var p1Shard6159Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001048-000000003.tsm", Size: 1385901489}},
	{FileStat: tsm1.FileStat{Path: "000001080-000000003.tsm", Size: 1432878032}},
	{FileStat: tsm1.FileStat{Path: "000001112-000000003.tsm", Size: 1318332062}},
	{FileStat: tsm1.FileStat{Path: "000001600-000000004.tsm", Size: 2147975701}},
	{FileStat: tsm1.FileStat{Path: "000002608-000000003.tsm", Size: 1435836359}},
	{FileStat: tsm1.FileStat{Path: "000002640-000000003.tsm", Size: 1410646965}},
	{FileStat: tsm1.FileStat{Path: "000002648-000000002.tsm", Size: 429753379}},
	{FileStat: tsm1.FileStat{Path: "000002656-000000002.tsm", Size: 338133013}},
	{FileStat: tsm1.FileStat{Path: "000002664-000000002.tsm", Size: 374565873}},
	{FileStat: tsm1.FileStat{Path: "000002672-000000018.tsm", Size: 2147574831}},
}

var p1Shard6161Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001545-000000003.tsm", Size: 1434045689}},
	{FileStat: tsm1.FileStat{Path: "000001585-000000003.tsm", Size: 1283154838}},
	{FileStat: tsm1.FileStat{Path: "000001617-000000003.tsm", Size: 1383312261}},
	{FileStat: tsm1.FileStat{Path: "000001625-000000002.tsm", Size: 393163647}},
	{FileStat: tsm1.FileStat{Path: "000001633-000000002.tsm", Size: 416868048}},
	{FileStat: tsm1.FileStat{Path: "000001641-000000002.tsm", Size: 372884586}},
	{FileStat: tsm1.FileStat{Path: "000001649-000000002.tsm", Size: 401237164}},
	{FileStat: tsm1.FileStat{Path: "000001657-000000002.tsm", Size: 380194995}},
	{FileStat: tsm1.FileStat{Path: "000001665-000000002.tsm", Size: 385501127}},
	{FileStat: tsm1.FileStat{Path: "000001673-000000002.tsm", Size: 381790341}},
	{FileStat: tsm1.FileStat{Path: "000001681-000000002.tsm", Size: 377108321}},
	{FileStat: tsm1.FileStat{Path: "000001689-000000002.tsm", Size: 415983678}},
	{FileStat: tsm1.FileStat{Path: "000001697-000000002.tsm", Size: 375708145}},
	{FileStat: tsm1.FileStat{Path: "000001705-000000002.tsm", Size: 405509566}},
	{FileStat: tsm1.FileStat{Path: "000001713-000000002.tsm", Size: 381448886}},
	{FileStat: tsm1.FileStat{Path: "000001729-000000002.tsm", Size: 402499566}},
	{FileStat: tsm1.FileStat{Path: "000001737-000000002.tsm", Size: 376503742}},
	{FileStat: tsm1.FileStat{Path: "000001745-000000002.tsm", Size: 499900133}},
	{FileStat: tsm1.FileStat{Path: "000001753-000000002.tsm", Size: 348382501}},
	{FileStat: tsm1.FileStat{Path: "000001761-000000002.tsm", Size: 368189986}},
	{FileStat: tsm1.FileStat{Path: "000001769-000000002.tsm", Size: 391133580}},
	{FileStat: tsm1.FileStat{Path: "000001777-000000002.tsm", Size: 412056805}},
	{FileStat: tsm1.FileStat{Path: "000001785-000000002.tsm", Size: 386975681}},
	{FileStat: tsm1.FileStat{Path: "000001793-000000002.tsm", Size: 380775153}},
	{FileStat: tsm1.FileStat{Path: "000001801-000000002.tsm", Size: 415344938}},
	{FileStat: tsm1.FileStat{Path: "000001809-000000002.tsm", Size: 349723076}},
	{FileStat: tsm1.FileStat{Path: "000001817-000000002.tsm", Size: 399135642}},
	{FileStat: tsm1.FileStat{Path: "000001825-000000002.tsm", Size: 396885085}},
	{FileStat: tsm1.FileStat{Path: "000001833-000000002.tsm", Size: 400940599}},
	{FileStat: tsm1.FileStat{Path: "000001841-000000002.tsm", Size: 416817268}},
	{FileStat: tsm1.FileStat{Path: "000001849-000000002.tsm", Size: 380057892}},
	{FileStat: tsm1.FileStat{Path: "000001865-000000002.tsm", Size: 367233594}},
	{FileStat: tsm1.FileStat{Path: "000001873-000000002.tsm", Size: 385691779}},
	{FileStat: tsm1.FileStat{Path: "000001881-000000002.tsm", Size: 409126913}},
	{FileStat: tsm1.FileStat{Path: "000001889-000000002.tsm", Size: 414740761}},
	{FileStat: tsm1.FileStat{Path: "000001897-000000002.tsm", Size: 352754699}},
	{FileStat: tsm1.FileStat{Path: "000001905-000000002.tsm", Size: 396201446}},
	{FileStat: tsm1.FileStat{Path: "000001913-000000002.tsm", Size: 389010767}},
	{FileStat: tsm1.FileStat{Path: "000001921-000000002.tsm", Size: 425891814}},
	{FileStat: tsm1.FileStat{Path: "000001929-000000002.tsm", Size: 354306957}},
	{FileStat: tsm1.FileStat{Path: "000001937-000000002.tsm", Size: 387477818}},
	{FileStat: tsm1.FileStat{Path: "000001945-000000002.tsm", Size: 398321970}},
	{FileStat: tsm1.FileStat{Path: "000001953-000000002.tsm", Size: 402924470}},
	{FileStat: tsm1.FileStat{Path: "000001961-000000002.tsm", Size: 369808482}},
	{FileStat: tsm1.FileStat{Path: "000001969-000000002.tsm", Size: 399066773}},
	{FileStat: tsm1.FileStat{Path: "000001977-000000002.tsm", Size: 411774709}},
	{FileStat: tsm1.FileStat{Path: "000001985-000000002.tsm", Size: 430703447}},
	{FileStat: tsm1.FileStat{Path: "000001993-000000002.tsm", Size: 311496873}},
	{FileStat: tsm1.FileStat{Path: "000002001-000000002.tsm", Size: 395059766}},
	{FileStat: tsm1.FileStat{Path: "000002009-000000002.tsm", Size: 433538020}},
	{FileStat: tsm1.FileStat{Path: "000002017-000000002.tsm", Size: 372108927}},
	{FileStat: tsm1.FileStat{Path: "000002025-000000002.tsm", Size: 413810399}},
	{FileStat: tsm1.FileStat{Path: "000002033-000000002.tsm", Size: 351818189}},
	{FileStat: tsm1.FileStat{Path: "000002041-000000002.tsm", Size: 403633327}},
	{FileStat: tsm1.FileStat{Path: "000002049-000000002.tsm", Size: 367651661}},
	{FileStat: tsm1.FileStat{Path: "000002057-000000002.tsm", Size: 438861316}},
	{FileStat: tsm1.FileStat{Path: "000002065-000000002.tsm", Size: 349168662}},
	{FileStat: tsm1.FileStat{Path: "000002073-000000002.tsm", Size: 366381466}},
	{FileStat: tsm1.FileStat{Path: "000002081-000000002.tsm", Size: 397496800}},
	{FileStat: tsm1.FileStat{Path: "000002089-000000002.tsm", Size: 396404318}},
	{FileStat: tsm1.FileStat{Path: "000002097-000000002.tsm", Size: 400431473}},
	{FileStat: tsm1.FileStat{Path: "000002105-000000002.tsm", Size: 366310681}},
	{FileStat: tsm1.FileStat{Path: "000002113-000000002.tsm", Size: 379333203}},
	{FileStat: tsm1.FileStat{Path: "000002114-000000001.tsm", Size: 70359219}},
	{FileStat: tsm1.FileStat{Path: "000002115-000000001.tsm", Size: 65962650}},
	{FileStat: tsm1.FileStat{Path: "000002116-000000001.tsm", Size: 63441797}},
	{FileStat: tsm1.FileStat{Path: "000002119-000000001.tsm", Size: 81494039}},
	{FileStat: tsm1.FileStat{Path: "000002120-000000001.tsm", Size: 62637887}},
	{FileStat: tsm1.FileStat{Path: "000002121-000000001.tsm", Size: 64323325}},
	{FileStat: tsm1.FileStat{Path: "000004402-000000004.tsm", Size: 2147570672}},
}

var p1Shard6162Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001056-000000003.tsm", Size: 1378796439}},
	{FileStat: tsm1.FileStat{Path: "000001088-000000003.tsm", Size: 1446473618}},
	{FileStat: tsm1.FileStat{Path: "000001120-000000003.tsm", Size: 1405128079}},
	{FileStat: tsm1.FileStat{Path: "000001152-000000003.tsm", Size: 1543518831}},
	{FileStat: tsm1.FileStat{Path: "000001184-000000003.tsm", Size: 1395136897}},
	{FileStat: tsm1.FileStat{Path: "000001216-000000003.tsm", Size: 1427972729}},
	{FileStat: tsm1.FileStat{Path: "000001248-000000003.tsm", Size: 1440409152}},
	{FileStat: tsm1.FileStat{Path: "000001280-000000003.tsm", Size: 1416819968}},
	{FileStat: tsm1.FileStat{Path: "000001312-000000003.tsm", Size: 1432581344}},
	{FileStat: tsm1.FileStat{Path: "000001352-000000003.tsm", Size: 1437996016}},
	{FileStat: tsm1.FileStat{Path: "000001360-000000002.tsm", Size: 420417048}},
	{FileStat: tsm1.FileStat{Path: "000001368-000000002.tsm", Size: 391529167}},
	{FileStat: tsm1.FileStat{Path: "000001376-000000002.tsm", Size: 369142142}},
	{FileStat: tsm1.FileStat{Path: "000001384-000000002.tsm", Size: 398311693}},
	{FileStat: tsm1.FileStat{Path: "000001392-000000002.tsm", Size: 371499343}},
	{FileStat: tsm1.FileStat{Path: "000001400-000000002.tsm", Size: 406313202}},
	{FileStat: tsm1.FileStat{Path: "000001408-000000002.tsm", Size: 364927392}},
	{FileStat: tsm1.FileStat{Path: "000001416-000000002.tsm", Size: 363005800}},
	{FileStat: tsm1.FileStat{Path: "000001424-000000002.tsm", Size: 463610634}},
	{FileStat: tsm1.FileStat{Path: "000001432-000000002.tsm", Size: 348782282}},
	{FileStat: tsm1.FileStat{Path: "000001440-000000002.tsm", Size: 388119818}},
	{FileStat: tsm1.FileStat{Path: "000001448-000000002.tsm", Size: 441010992}},
	{FileStat: tsm1.FileStat{Path: "000001456-000000002.tsm", Size: 378461155}},
	{FileStat: tsm1.FileStat{Path: "000001464-000000002.tsm", Size: 368314909}},
	{FileStat: tsm1.FileStat{Path: "000001472-000000002.tsm", Size: 421715992}},
	{FileStat: tsm1.FileStat{Path: "000001480-000000002.tsm", Size: 396350178}},
	{FileStat: tsm1.FileStat{Path: "000001488-000000002.tsm", Size: 406571709}},
	{FileStat: tsm1.FileStat{Path: "000001496-000000002.tsm", Size: 386868692}},
	{FileStat: tsm1.FileStat{Path: "000001504-000000002.tsm", Size: 383477327}},
	{FileStat: tsm1.FileStat{Path: "000001512-000000002.tsm", Size: 422939083}},
	{FileStat: tsm1.FileStat{Path: "000001520-000000002.tsm", Size: 365697156}},
	{FileStat: tsm1.FileStat{Path: "000001528-000000002.tsm", Size: 382732669}},
	{FileStat: tsm1.FileStat{Path: "000001536-000000002.tsm", Size: 431684809}},
	{FileStat: tsm1.FileStat{Path: "000001544-000000002.tsm", Size: 352695886}},
	{FileStat: tsm1.FileStat{Path: "000001552-000000002.tsm", Size: 388238090}},
	{FileStat: tsm1.FileStat{Path: "000001560-000000002.tsm", Size: 404358529}},
	{FileStat: tsm1.FileStat{Path: "000001568-000000002.tsm", Size: 388985059}},
	{FileStat: tsm1.FileStat{Path: "000001576-000000002.tsm", Size: 390668607}},
	{FileStat: tsm1.FileStat{Path: "000001584-000000002.tsm", Size: 397907191}},
	{FileStat: tsm1.FileStat{Path: "000001592-000000002.tsm", Size: 373949195}},
	{FileStat: tsm1.FileStat{Path: "000001600-000000002.tsm", Size: 389853458}},
	{FileStat: tsm1.FileStat{Path: "000001608-000000002.tsm", Size: 413645715}},
	{FileStat: tsm1.FileStat{Path: "000001616-000000002.tsm", Size: 378577559}},
	{FileStat: tsm1.FileStat{Path: "000001624-000000002.tsm", Size: 380869598}},
	{FileStat: tsm1.FileStat{Path: "000001632-000000002.tsm", Size: 385482406}},
	{FileStat: tsm1.FileStat{Path: "000001640-000000002.tsm", Size: 394707721}},
	{FileStat: tsm1.FileStat{Path: "000001648-000000002.tsm", Size: 387655129}},
	{FileStat: tsm1.FileStat{Path: "000001656-000000002.tsm", Size: 386595871}},
	{FileStat: tsm1.FileStat{Path: "000001664-000000002.tsm", Size: 381527665}},
	{FileStat: tsm1.FileStat{Path: "000001672-000000002.tsm", Size: 387938505}},
	{FileStat: tsm1.FileStat{Path: "000001680-000000002.tsm", Size: 392654357}},
	{FileStat: tsm1.FileStat{Path: "000001681-000000001.tsm", Size: 63644141}},
	{FileStat: tsm1.FileStat{Path: "000001682-000000001.tsm", Size: 63297555}},
	{FileStat: tsm1.FileStat{Path: "000001683-000000001.tsm", Size: 77391438}},
	{FileStat: tsm1.FileStat{Path: "000001684-000000001.tsm", Size: 59429174}},
	{FileStat: tsm1.FileStat{Path: "000001685-000000001.tsm", Size: 64693261}},
	{FileStat: tsm1.FileStat{Path: "000001686-000000001.tsm", Size: 58595647}},
	{FileStat: tsm1.FileStat{Path: "000001687-000000001.tsm", Size: 55259523}},
	{FileStat: tsm1.FileStat{Path: "000001688-000000001.tsm", Size: 62271817}},
	{FileStat: tsm1.FileStat{Path: "000004401-000000004.tsm", Size: 2148068669}},
}

var p1Shard6164Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000504-000000003.tsm", Size: 1420897977}},
	{FileStat: tsm1.FileStat{Path: "000000536-000000003.tsm", Size: 1399456916}},
	{FileStat: tsm1.FileStat{Path: "000000568-000000003.tsm", Size: 1402320281}},
	{FileStat: tsm1.FileStat{Path: "000000600-000000003.tsm", Size: 1440398301}},
	{FileStat: tsm1.FileStat{Path: "000000632-000000003.tsm", Size: 1392080015}},
	{FileStat: tsm1.FileStat{Path: "000000672-000000003.tsm", Size: 1481628808}},
	{FileStat: tsm1.FileStat{Path: "000000680-000000002.tsm", Size: 357052007}},
	{FileStat: tsm1.FileStat{Path: "000000688-000000002.tsm", Size: 383949695}},
	{FileStat: tsm1.FileStat{Path: "000000696-000000002.tsm", Size: 405674109}},
	{FileStat: tsm1.FileStat{Path: "000000704-000000002.tsm", Size: 347226023}},
	{FileStat: tsm1.FileStat{Path: "000000712-000000002.tsm", Size: 401004389}},
	{FileStat: tsm1.FileStat{Path: "000000720-000000002.tsm", Size: 373891658}},
	{FileStat: tsm1.FileStat{Path: "000000728-000000002.tsm", Size: 408762948}},
	{FileStat: tsm1.FileStat{Path: "000000736-000000002.tsm", Size: 318384682}},
	{FileStat: tsm1.FileStat{Path: "000000744-000000002.tsm", Size: 380781016}},
	{FileStat: tsm1.FileStat{Path: "000000752-000000002.tsm", Size: 401723899}},
	{FileStat: tsm1.FileStat{Path: "000000760-000000002.tsm", Size: 430202832}},
	{FileStat: tsm1.FileStat{Path: "000000768-000000002.tsm", Size: 375113802}},
	{FileStat: tsm1.FileStat{Path: "000000776-000000002.tsm", Size: 358226751}},
	{FileStat: tsm1.FileStat{Path: "000000784-000000002.tsm", Size: 467717566}},
	{FileStat: tsm1.FileStat{Path: "000000792-000000002.tsm", Size: 370559801}},
	{FileStat: tsm1.FileStat{Path: "000000800-000000002.tsm", Size: 412010784}},
	{FileStat: tsm1.FileStat{Path: "000000801-000000001.tsm", Size: 72616156}},
	{FileStat: tsm1.FileStat{Path: "000000802-000000001.tsm", Size: 70057087}},
	{FileStat: tsm1.FileStat{Path: "000000803-000000001.tsm", Size: 65618101}},
	{FileStat: tsm1.FileStat{Path: "000000804-000000001.tsm", Size: 71243973}},
	{FileStat: tsm1.FileStat{Path: "000000805-000000001.tsm", Size: 68942945}},
	{FileStat: tsm1.FileStat{Path: "000000806-000000001.tsm", Size: 69486865}},
	{FileStat: tsm1.FileStat{Path: "000000807-000000001.tsm", Size: 72245063}},
	{FileStat: tsm1.FileStat{Path: "000000808-000000001.tsm", Size: 39740515}},
	{FileStat: tsm1.FileStat{Path: "000000944-000000004.tsm", Size: 2148050841}},
}

var p2Shard6147Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000002089-000000003.tsm", Size: 863582961}},
	{FileStat: tsm1.FileStat{Path: "000002097-000000002.tsm", Size: 350374853}},
	{FileStat: tsm1.FileStat{Path: "000002105-000000002.tsm", Size: 343218425}},
	{FileStat: tsm1.FileStat{Path: "000002113-000000002.tsm", Size: 379035685}},
	{FileStat: tsm1.FileStat{Path: "000002121-000000002.tsm", Size: 345593838}},
	{FileStat: tsm1.FileStat{Path: "000002129-000000002.tsm", Size: 365073537}},
	{FileStat: tsm1.FileStat{Path: "000002137-000000002.tsm", Size: 385590187}},
	{FileStat: tsm1.FileStat{Path: "000002138-000000001.tsm", Size: 62751745}},
	{FileStat: tsm1.FileStat{Path: "000002139-000000001.tsm", Size: 52736091}},
	{FileStat: tsm1.FileStat{Path: "000002140-000000001.tsm", Size: 61281716}},
	{FileStat: tsm1.FileStat{Path: "000002141-000000001.tsm", Size: 60498054}},
	{FileStat: tsm1.FileStat{Path: "000002142-000000001.tsm", Size: 73929516}},
	{FileStat: tsm1.FileStat{Path: "000002143-000000001.tsm", Size: 63748530}},
	{FileStat: tsm1.FileStat{Path: "000002144-000000001.tsm", Size: 58551989}},
	{FileStat: tsm1.FileStat{Path: "000002145-000000001.tsm", Size: 65140775}},
	{FileStat: tsm1.FileStat{Path: "000004707-000000032.tsm", Size: 2147532811}},
}

var p2Shard6158Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000002984-000000003.tsm", Size: 1367996258}},
	{FileStat: tsm1.FileStat{Path: "000003016-000000003.tsm", Size: 1352711054}},
	{FileStat: tsm1.FileStat{Path: "000003048-000000003.tsm", Size: 1318660484}},
	{FileStat: tsm1.FileStat{Path: "000003080-000000003.tsm", Size: 1368591700}},
	{FileStat: tsm1.FileStat{Path: "000003112-000000003.tsm", Size: 1443994761}},
	{FileStat: tsm1.FileStat{Path: "000003120-000000002.tsm", Size: 348880921}},
	{FileStat: tsm1.FileStat{Path: "000003128-000000002.tsm", Size: 363828835}},
	{FileStat: tsm1.FileStat{Path: "000003136-000000002.tsm", Size: 373098181}},
	{FileStat: tsm1.FileStat{Path: "000003144-000000002.tsm", Size: 372320597}},
	{FileStat: tsm1.FileStat{Path: "000003152-000000002.tsm", Size: 425982464}},
	{FileStat: tsm1.FileStat{Path: "000003160-000000002.tsm", Size: 334289890}},
	{FileStat: tsm1.FileStat{Path: "000003168-000000002.tsm", Size: 367888472}},
	{FileStat: tsm1.FileStat{Path: "000003176-000000002.tsm", Size: 376528085}},
	{FileStat: tsm1.FileStat{Path: "000003184-000000002.tsm", Size: 385321040}},
	{FileStat: tsm1.FileStat{Path: "000003192-000000002.tsm", Size: 390853804}},
	{FileStat: tsm1.FileStat{Path: "000003200-000000002.tsm", Size: 365310018}},
	{FileStat: tsm1.FileStat{Path: "000003208-000000002.tsm", Size: 375143396}},
	{FileStat: tsm1.FileStat{Path: "000003216-000000002.tsm", Size: 391836729}},
	{FileStat: tsm1.FileStat{Path: "000003224-000000002.tsm", Size: 413192401}},
	{FileStat: tsm1.FileStat{Path: "000003232-000000002.tsm", Size: 344597242}},
	{FileStat: tsm1.FileStat{Path: "000003240-000000002.tsm", Size: 389404249}},
	{FileStat: tsm1.FileStat{Path: "000003248-000000002.tsm", Size: 367158911}},
	{FileStat: tsm1.FileStat{Path: "000003256-000000002.tsm", Size: 356857192}},
	{FileStat: tsm1.FileStat{Path: "000003264-000000002.tsm", Size: 384814895}},
	{FileStat: tsm1.FileStat{Path: "000003272-000000002.tsm", Size: 389685584}},
	{FileStat: tsm1.FileStat{Path: "000003280-000000002.tsm", Size: 397530300}},
	{FileStat: tsm1.FileStat{Path: "000003288-000000002.tsm", Size: 391365943}},
	{FileStat: tsm1.FileStat{Path: "000003296-000000002.tsm", Size: 448591168}},
	{FileStat: tsm1.FileStat{Path: "000003304-000000002.tsm", Size: 358033700}},
	{FileStat: tsm1.FileStat{Path: "000003312-000000002.tsm", Size: 376631278}},
	{FileStat: tsm1.FileStat{Path: "000003320-000000002.tsm", Size: 403400812}},
	{FileStat: tsm1.FileStat{Path: "000003328-000000002.tsm", Size: 377112727}},
	{FileStat: tsm1.FileStat{Path: "000003336-000000002.tsm", Size: 400655683}},
	{FileStat: tsm1.FileStat{Path: "000003344-000000002.tsm", Size: 360354384}},
	{FileStat: tsm1.FileStat{Path: "000003352-000000002.tsm", Size: 375622779}},
	{FileStat: tsm1.FileStat{Path: "000003360-000000002.tsm", Size: 403207935}},
	{FileStat: tsm1.FileStat{Path: "000003368-000000002.tsm", Size: 392534273}},
	{FileStat: tsm1.FileStat{Path: "000003376-000000002.tsm", Size: 355400735}},
	{FileStat: tsm1.FileStat{Path: "000003384-000000002.tsm", Size: 408463521}},
	{FileStat: tsm1.FileStat{Path: "000003392-000000002.tsm", Size: 372456469}},
	{FileStat: tsm1.FileStat{Path: "000003400-000000002.tsm", Size: 429916003}},
	{FileStat: tsm1.FileStat{Path: "000003408-000000002.tsm", Size: 348325356}},
	{FileStat: tsm1.FileStat{Path: "000003416-000000002.tsm", Size: 376299144}},
	{FileStat: tsm1.FileStat{Path: "000003424-000000002.tsm", Size: 371491422}},
	{FileStat: tsm1.FileStat{Path: "000003432-000000002.tsm", Size: 451468382}},
	{FileStat: tsm1.FileStat{Path: "000003440-000000002.tsm", Size: 306829597}},
	{FileStat: tsm1.FileStat{Path: "000003448-000000002.tsm", Size: 380379385}},
	{FileStat: tsm1.FileStat{Path: "000003456-000000002.tsm", Size: 396922571}},
	{FileStat: tsm1.FileStat{Path: "000003464-000000002.tsm", Size: 414666443}},
	{FileStat: tsm1.FileStat{Path: "000003472-000000002.tsm", Size: 352166508}},
	{FileStat: tsm1.FileStat{Path: "000003480-000000002.tsm", Size: 394648697}},
	{FileStat: tsm1.FileStat{Path: "000003488-000000002.tsm", Size: 385330817}},
	{FileStat: tsm1.FileStat{Path: "000003496-000000002.tsm", Size: 411572663}},
	{FileStat: tsm1.FileStat{Path: "000003504-000000002.tsm", Size: 355484678}},
	{FileStat: tsm1.FileStat{Path: "000003512-000000002.tsm", Size: 375186504}},
	{FileStat: tsm1.FileStat{Path: "000003520-000000002.tsm", Size: 378440325}},
	{FileStat: tsm1.FileStat{Path: "000003528-000000002.tsm", Size: 433445560}},
	{FileStat: tsm1.FileStat{Path: "000003536-000000002.tsm", Size: 359806075}},
	{FileStat: tsm1.FileStat{Path: "000003544-000000002.tsm", Size: 454155210}},
	{FileStat: tsm1.FileStat{Path: "000003552-000000002.tsm", Size: 382686687}},
	{FileStat: tsm1.FileStat{Path: "000003560-000000002.tsm", Size: 443640888}},
	{FileStat: tsm1.FileStat{Path: "000003592-000000002.tsm", Size: 409452154}},
	{FileStat: tsm1.FileStat{Path: "000003600-000000002.tsm", Size: 350997025}},
	{FileStat: tsm1.FileStat{Path: "000003608-000000002.tsm", Size: 395116331}},
	{FileStat: tsm1.FileStat{Path: "000003616-000000002.tsm", Size: 395055201}},
	{FileStat: tsm1.FileStat{Path: "000003624-000000002.tsm", Size: 408936561}},
	{FileStat: tsm1.FileStat{Path: "000003632-000000002.tsm", Size: 369866224}},
	{FileStat: tsm1.FileStat{Path: "000003640-000000002.tsm", Size: 417690715}},
	{FileStat: tsm1.FileStat{Path: "000003648-000000002.tsm", Size: 398328092}},
	{FileStat: tsm1.FileStat{Path: "000003656-000000002.tsm", Size: 357662667}},
	{FileStat: tsm1.FileStat{Path: "000003664-000000002.tsm", Size: 403885705}},
	{FileStat: tsm1.FileStat{Path: "000003672-000000002.tsm", Size: 375042918}},
	{FileStat: tsm1.FileStat{Path: "000003680-000000002.tsm", Size: 447383682}},
	{FileStat: tsm1.FileStat{Path: "000003688-000000002.tsm", Size: 347866792}},
	{FileStat: tsm1.FileStat{Path: "000003689-000000001.tsm", Size: 62378766}},
	{FileStat: tsm1.FileStat{Path: "000003690-000000001.tsm", Size: 60988418}},
	{FileStat: tsm1.FileStat{Path: "000003691-000000001.tsm", Size: 60833633}},
	{FileStat: tsm1.FileStat{Path: "000003692-000000001.tsm", Size: 61430172}},
	{FileStat: tsm1.FileStat{Path: "000003693-000000001.tsm", Size: 71130047}},
	{FileStat: tsm1.FileStat{Path: "000003694-000000001.tsm", Size: 78442632}},
	{FileStat: tsm1.FileStat{Path: "000003695-000000001.tsm", Size: 63163193}},
	{FileStat: tsm1.FileStat{Path: "000003696-000000001.tsm", Size: 63923209}},
	{FileStat: tsm1.FileStat{Path: "000004508-000000035.tsm", Size: 2147529111}},
}

var p2Shard6159Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000002608-000000003.tsm", Size: 1418177704}},
	{FileStat: tsm1.FileStat{Path: "000002640-000000003.tsm", Size: 1436583114}},
	{FileStat: tsm1.FileStat{Path: "000002648-000000002.tsm", Size: 382914646}},
	{FileStat: tsm1.FileStat{Path: "000002656-000000002.tsm", Size: 382677419}},
	{FileStat: tsm1.FileStat{Path: "000002664-000000002.tsm", Size: 375041644}},
	{FileStat: tsm1.FileStat{Path: "000002665-000000001.tsm", Size: 62290187}},
	{FileStat: tsm1.FileStat{Path: "000002666-000000001.tsm", Size: 60198335}},
	{FileStat: tsm1.FileStat{Path: "000002667-000000001.tsm", Size: 62519002}},
	{FileStat: tsm1.FileStat{Path: "000002668-000000001.tsm", Size: 78370679}},
	{FileStat: tsm1.FileStat{Path: "000002669-000000001.tsm", Size: 70161211}},
	{FileStat: tsm1.FileStat{Path: "000002670-000000001.tsm", Size: 57995277}},
	{FileStat: tsm1.FileStat{Path: "000002671-000000001.tsm", Size: 62997782}},
	{FileStat: tsm1.FileStat{Path: "000002672-000000001.tsm", Size: 60222084}},
	{FileStat: tsm1.FileStat{Path: "000004396-000000047.tsm", Size: 2147538824}},
}

var p2Shard6160Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000002753-000000003.tsm", Size: 1414508234}},
	{FileStat: tsm1.FileStat{Path: "000002785-000000003.tsm", Size: 1390102578}},
	{FileStat: tsm1.FileStat{Path: "000002817-000000003.tsm", Size: 1407872044}},
	{FileStat: tsm1.FileStat{Path: "000002849-000000003.tsm", Size: 1379094119}},
	{FileStat: tsm1.FileStat{Path: "000002881-000000003.tsm", Size: 1375559774}},
	{FileStat: tsm1.FileStat{Path: "000002913-000000003.tsm", Size: 1422573323}},
	{FileStat: tsm1.FileStat{Path: "000002945-000000003.tsm", Size: 1419785735}},
	{FileStat: tsm1.FileStat{Path: "000002977-000000003.tsm", Size: 1445303360}},
	{FileStat: tsm1.FileStat{Path: "000003009-000000003.tsm", Size: 1435791466}},
	{FileStat: tsm1.FileStat{Path: "000003041-000000003.tsm", Size: 1527802933}},
	{FileStat: tsm1.FileStat{Path: "000003073-000000003.tsm", Size: 1523252671}},
	{FileStat: tsm1.FileStat{Path: "000003105-000000003.tsm", Size: 1512589815}},
	{FileStat: tsm1.FileStat{Path: "000003137-000000003.tsm", Size: 1449530683}},
	{FileStat: tsm1.FileStat{Path: "000003169-000000003.tsm", Size: 1412812328}},
	{FileStat: tsm1.FileStat{Path: "000003201-000000003.tsm", Size: 1430804673}},
	{FileStat: tsm1.FileStat{Path: "000003233-000000003.tsm", Size: 1424195487}},
	{FileStat: tsm1.FileStat{Path: "000003265-000000003.tsm", Size: 1405927596}},
	{FileStat: tsm1.FileStat{Path: "000003297-000000003.tsm", Size: 1441388553}},
	{FileStat: tsm1.FileStat{Path: "000003329-000000003.tsm", Size: 1425837957}},
	{FileStat: tsm1.FileStat{Path: "000003361-000000003.tsm", Size: 1422178995}},
	{FileStat: tsm1.FileStat{Path: "000003393-000000003.tsm", Size: 1441211928}},
	{FileStat: tsm1.FileStat{Path: "000003425-000000003.tsm", Size: 1445159091}},
	{FileStat: tsm1.FileStat{Path: "000003465-000000003.tsm", Size: 1474017886}},
	{FileStat: tsm1.FileStat{Path: "000003497-000000003.tsm", Size: 1422683242}},
	{FileStat: tsm1.FileStat{Path: "000003529-000000003.tsm", Size: 1431956100}},
	{FileStat: tsm1.FileStat{Path: "000003561-000000003.tsm", Size: 1500065608}},
	{FileStat: tsm1.FileStat{Path: "000003593-000000003.tsm", Size: 1432560438}},
	{FileStat: tsm1.FileStat{Path: "000003625-000000003.tsm", Size: 1480005223}},
	{FileStat: tsm1.FileStat{Path: "000003657-000000003.tsm", Size: 1416980598}},
	{FileStat: tsm1.FileStat{Path: "000003665-000000004.tsm", Size: 2147836853}},
}

var p2Shard6164Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000512-000000003.tsm", Size: 1405207551}},
	{FileStat: tsm1.FileStat{Path: "000000544-000000003.tsm", Size: 1397138279}},
	{FileStat: tsm1.FileStat{Path: "000000576-000000004.tsm", Size: 2147924181}},
}

var p3Shard6156Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000003120-000000003.tsm", Size: 1494662101}},
	{FileStat: tsm1.FileStat{Path: "000003152-000000003.tsm", Size: 1420802809}},
	{FileStat: tsm1.FileStat{Path: "000003184-000000003.tsm", Size: 1422061594}},
	{FileStat: tsm1.FileStat{Path: "000003216-000000003.tsm", Size: 1455375892}},
	{FileStat: tsm1.FileStat{Path: "000003248-000000003.tsm", Size: 1443861870}},
	{FileStat: tsm1.FileStat{Path: "000003256-000000002.tsm", Size: 398150333}},
	{FileStat: tsm1.FileStat{Path: "000003264-000000002.tsm", Size: 384572579}},
	{FileStat: tsm1.FileStat{Path: "000003272-000000002.tsm", Size: 306923197}},
	{FileStat: tsm1.FileStat{Path: "000003280-000000002.tsm", Size: 404709261}},
	{FileStat: tsm1.FileStat{Path: "000003288-000000002.tsm", Size: 369552950}},
	{FileStat: tsm1.FileStat{Path: "000003296-000000002.tsm", Size: 344666607}},
	{FileStat: tsm1.FileStat{Path: "000003304-000000002.tsm", Size: 405261271}},
	{FileStat: tsm1.FileStat{Path: "000003312-000000002.tsm", Size: 379792854}},
	{FileStat: tsm1.FileStat{Path: "000003313-000000001.tsm", Size: 81551586}},
	{FileStat: tsm1.FileStat{Path: "000003314-000000001.tsm", Size: 64297182}},
	{FileStat: tsm1.FileStat{Path: "000003315-000000001.tsm", Size: 61892743}},
	{FileStat: tsm1.FileStat{Path: "000003316-000000001.tsm", Size: 56180528}},
	{FileStat: tsm1.FileStat{Path: "000003317-000000001.tsm", Size: 59692159}},
	{FileStat: tsm1.FileStat{Path: "000003318-000000001.tsm", Size: 48204015}},
	{FileStat: tsm1.FileStat{Path: "000003319-000000001.tsm", Size: 44842226}},
	{FileStat: tsm1.FileStat{Path: "000003320-000000001.tsm", Size: 64750840}},
	{FileStat: tsm1.FileStat{Path: "000004980-000000099.tsm", Size: 2147520950}},
}

var p3Shard6159Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001664-000000003.tsm", Size: 1430429962}},
	{FileStat: tsm1.FileStat{Path: "000001696-000000003.tsm", Size: 1408764310}},
	{FileStat: tsm1.FileStat{Path: "000001728-000000003.tsm", Size: 1499436040}},
	{FileStat: tsm1.FileStat{Path: "000001760-000000003.tsm", Size: 1442029688}},
	{FileStat: tsm1.FileStat{Path: "000001768-000000002.tsm", Size: 385987159}},
	{FileStat: tsm1.FileStat{Path: "000001776-000000002.tsm", Size: 386394057}},
	{FileStat: tsm1.FileStat{Path: "000001784-000000002.tsm", Size: 411439845}},
	{FileStat: tsm1.FileStat{Path: "000001792-000000002.tsm", Size: 354545514}},
	{FileStat: tsm1.FileStat{Path: "000001800-000000002.tsm", Size: 392458766}},
	{FileStat: tsm1.FileStat{Path: "000001808-000000002.tsm", Size: 432906908}},
	{FileStat: tsm1.FileStat{Path: "000001816-000000002.tsm", Size: 400515609}},
	{FileStat: tsm1.FileStat{Path: "000001824-000000002.tsm", Size: 371412714}},
	{FileStat: tsm1.FileStat{Path: "000001832-000000002.tsm", Size: 434735277}},
	{FileStat: tsm1.FileStat{Path: "000001840-000000002.tsm", Size: 408509094}},
	{FileStat: tsm1.FileStat{Path: "000001848-000000002.tsm", Size: 369334466}},
	{FileStat: tsm1.FileStat{Path: "000001856-000000002.tsm", Size: 409175081}},
	{FileStat: tsm1.FileStat{Path: "000001864-000000002.tsm", Size: 376855878}},
	{FileStat: tsm1.FileStat{Path: "000001872-000000002.tsm", Size: 399987776}},
	{FileStat: tsm1.FileStat{Path: "000001880-000000002.tsm", Size: 403227857}},
	{FileStat: tsm1.FileStat{Path: "000001888-000000002.tsm", Size: 382694247}},
	{FileStat: tsm1.FileStat{Path: "000001896-000000002.tsm", Size: 395491623}},
	{FileStat: tsm1.FileStat{Path: "000001904-000000002.tsm", Size: 406576263}},
	{FileStat: tsm1.FileStat{Path: "000001912-000000002.tsm", Size: 367654849}},
	{FileStat: tsm1.FileStat{Path: "000001913-000000001.tsm", Size: 63812423}},
	{FileStat: tsm1.FileStat{Path: "000001914-000000001.tsm", Size: 61870681}},
	{FileStat: tsm1.FileStat{Path: "000001915-000000001.tsm", Size: 83076525}},
	{FileStat: tsm1.FileStat{Path: "000001916-000000001.tsm", Size: 66483912}},
	{FileStat: tsm1.FileStat{Path: "000001917-000000001.tsm", Size: 58715799}},
	{FileStat: tsm1.FileStat{Path: "000001918-000000001.tsm", Size: 62607583}},
	{FileStat: tsm1.FileStat{Path: "000001919-000000001.tsm", Size: 58862230}},
	{FileStat: tsm1.FileStat{Path: "000001920-000000001.tsm", Size: 61459657}},
	{FileStat: tsm1.FileStat{Path: "000003712-000000007.tsm", Size: 2147575885}},
}

var p3Shard6161Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001225-000000003.tsm", Size: 1383998127}},
	{FileStat: tsm1.FileStat{Path: "000001257-000000003.tsm", Size: 1461800194}},
	{FileStat: tsm1.FileStat{Path: "000001289-000000003.tsm", Size: 1397285073}},
	{FileStat: tsm1.FileStat{Path: "000001321-000000003.tsm", Size: 1490722730}},
	{FileStat: tsm1.FileStat{Path: "000001353-000000003.tsm", Size: 1400481044}},
	{FileStat: tsm1.FileStat{Path: "000001361-000000002.tsm", Size: 367043121}},
	{FileStat: tsm1.FileStat{Path: "000001369-000000002.tsm", Size: 407265287}},
	{FileStat: tsm1.FileStat{Path: "000001377-000000002.tsm", Size: 416653264}},
	{FileStat: tsm1.FileStat{Path: "000001385-000000002.tsm", Size: 352308834}},
	{FileStat: tsm1.FileStat{Path: "000001393-000000002.tsm", Size: 406441301}},
	{FileStat: tsm1.FileStat{Path: "000001401-000000002.tsm", Size: 425883332}},
	{FileStat: tsm1.FileStat{Path: "000001409-000000002.tsm", Size: 379217011}},
	{FileStat: tsm1.FileStat{Path: "000001417-000000002.tsm", Size: 421800741}},
	{FileStat: tsm1.FileStat{Path: "000001425-000000002.tsm", Size: 390600305}},
	{FileStat: tsm1.FileStat{Path: "000001433-000000002.tsm", Size: 381524260}},
	{FileStat: tsm1.FileStat{Path: "000001441-000000002.tsm", Size: 409439044}},
	{FileStat: tsm1.FileStat{Path: "000001449-000000002.tsm", Size: 387239902}},
	{FileStat: tsm1.FileStat{Path: "000001457-000000002.tsm", Size: 412846450}},
	{FileStat: tsm1.FileStat{Path: "000001465-000000002.tsm", Size: 425112753}},
	{FileStat: tsm1.FileStat{Path: "000001473-000000002.tsm", Size: 395162709}},
	{FileStat: tsm1.FileStat{Path: "000001481-000000002.tsm", Size: 374934076}},
	{FileStat: tsm1.FileStat{Path: "000001489-000000002.tsm", Size: 443024485}},
	{FileStat: tsm1.FileStat{Path: "000001497-000000002.tsm", Size: 348457093}},
	{FileStat: tsm1.FileStat{Path: "000001505-000000002.tsm", Size: 404932522}},
	{FileStat: tsm1.FileStat{Path: "000001513-000000004.tsm", Size: 2148072547}},
}

var p3Shard6162Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000800-000000003.tsm", Size: 1486939531}},
	{FileStat: tsm1.FileStat{Path: "000000856-000000003.tsm", Size: 1432908701}},
	{FileStat: tsm1.FileStat{Path: "000000888-000000003.tsm", Size: 1441885617}},
	{FileStat: tsm1.FileStat{Path: "000000920-000000007.tsm", Size: 2147641555}},
	{FileStat: tsm1.FileStat{Path: "000000928-000000002.tsm", Size: 381984573}},
	{FileStat: tsm1.FileStat{Path: "000000936-000000002.tsm", Size: 402694889}},
	{FileStat: tsm1.FileStat{Path: "000001064-000000004.tsm", Size: 2147932229}},
}

var p3Shard6163Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000344-000000002.tsm", Size: 351835380}},
	{FileStat: tsm1.FileStat{Path: "000000352-000000002.tsm", Size: 378959930}},
	{FileStat: tsm1.FileStat{Path: "000000360-000000002.tsm", Size: 423022249}},
	{FileStat: tsm1.FileStat{Path: "000000361-000000001.tsm", Size: 50200737}},
	{FileStat: tsm1.FileStat{Path: "000000362-000000001.tsm", Size: 68293487}},
	{FileStat: tsm1.FileStat{Path: "000000363-000000001.tsm", Size: 44783070}},
	{FileStat: tsm1.FileStat{Path: "000000364-000000001.tsm", Size: 46250158}},
	{FileStat: tsm1.FileStat{Path: "000000365-000000001.tsm", Size: 40942945}},
	{FileStat: tsm1.FileStat{Path: "000000366-000000001.tsm", Size: 59100686}},
	{FileStat: tsm1.FileStat{Path: "000000367-000000001.tsm", Size: 60378192}},
	{FileStat: tsm1.FileStat{Path: "000000368-000000001.tsm", Size: 87889049}},
	{FileStat: tsm1.FileStat{Path: "000005416-000000004.tsm", Size: 2148023683}},
}

var p3Shard6166Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000032-000000003.tsm", Size: 1681047481}},
	{FileStat: tsm1.FileStat{Path: "000000064-000000003.tsm", Size: 1444642933}},
	{FileStat: tsm1.FileStat{Path: "000000096-000000003.tsm", Size: 1427622811}},
	{FileStat: tsm1.FileStat{Path: "000000128-000000003.tsm", Size: 1486875493}},
	{FileStat: tsm1.FileStat{Path: "000000256-000000003.tsm", Size: 1385374813}},
	{FileStat: tsm1.FileStat{Path: "000000264-000000002.tsm", Size: 429404674}},
	{FileStat: tsm1.FileStat{Path: "000000272-000000002.tsm", Size: 368042109}},
	{FileStat: tsm1.FileStat{Path: "000000280-000000002.tsm", Size: 383605787}},
	{FileStat: tsm1.FileStat{Path: "000000288-000000002.tsm", Size: 445140438}},
	{FileStat: tsm1.FileStat{Path: "000000296-000000002.tsm", Size: 385680793}},
	{FileStat: tsm1.FileStat{Path: "000000304-000000002.tsm", Size: 374955673}},
	{FileStat: tsm1.FileStat{Path: "000000312-000000002.tsm", Size: 421235484}},
	{FileStat: tsm1.FileStat{Path: "000000320-000000002.tsm", Size: 363126807}},
	{FileStat: tsm1.FileStat{Path: "000000352-000000004.tsm", Size: 2147933711}},
}

var p4Shard6159Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001328-000000002.tsm", Size: 358675758}},
	{FileStat: tsm1.FileStat{Path: "000001336-000000002.tsm", Size: 366176515}},
	{FileStat: tsm1.FileStat{Path: "000001344-000000002.tsm", Size: 430141147}},
	{FileStat: tsm1.FileStat{Path: "000001352-000000002.tsm", Size: 360269010}},
	{FileStat: tsm1.FileStat{Path: "000001353-000000001.tsm", Size: 63836921}},
	{FileStat: tsm1.FileStat{Path: "000001354-000000001.tsm", Size: 76139760}},
	{FileStat: tsm1.FileStat{Path: "000001355-000000001.tsm", Size: 72650310}},
	{FileStat: tsm1.FileStat{Path: "000001356-000000001.tsm", Size: 61756290}},
	{FileStat: tsm1.FileStat{Path: "000001357-000000001.tsm", Size: 61671916}},
	{FileStat: tsm1.FileStat{Path: "000001358-000000001.tsm", Size: 63414447}},
	{FileStat: tsm1.FileStat{Path: "000001359-000000001.tsm", Size: 58554010}},
	{FileStat: tsm1.FileStat{Path: "000001360-000000001.tsm", Size: 65933734}},
	{FileStat: tsm1.FileStat{Path: "000004275-000000004.tsm", Size: 2148101920}},
}

var p4Shard6161Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000001545-000000003.tsm", Size: 1449436854}},
	{FileStat: tsm1.FileStat{Path: "000001553-000000002.tsm", Size: 405707531}},
	{FileStat: tsm1.FileStat{Path: "000001561-000000002.tsm", Size: 390708566}},
	{FileStat: tsm1.FileStat{Path: "000001562-000000001.tsm", Size: 53953825}},
	{FileStat: tsm1.FileStat{Path: "000001563-000000001.tsm", Size: 66157049}},
	{FileStat: tsm1.FileStat{Path: "000001564-000000001.tsm", Size: 62258877}},
	{FileStat: tsm1.FileStat{Path: "000001565-000000001.tsm", Size: 51234638}},
	{FileStat: tsm1.FileStat{Path: "000001566-000000001.tsm", Size: 53169736}},
	{FileStat: tsm1.FileStat{Path: "000001567-000000001.tsm", Size: 52823668}},
	{FileStat: tsm1.FileStat{Path: "000001568-000000001.tsm", Size: 64344562}},
	{FileStat: tsm1.FileStat{Path: "000001569-000000001.tsm", Size: 54683060}},
	{FileStat: tsm1.FileStat{Path: "000004025-000000004.tsm", Size: 2148063748}},
}

var p4Shard6162Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000992-000000003.tsm", Size: 1435889388}},
	{FileStat: tsm1.FileStat{Path: "000001000-000000002.tsm", Size: 420266013}},
	{FileStat: tsm1.FileStat{Path: "000001008-000000002.tsm", Size: 374292323}},
	{FileStat: tsm1.FileStat{Path: "000001016-000000002.tsm", Size: 410728411}},
	{FileStat: tsm1.FileStat{Path: "000001024-000000002.tsm", Size: 384588842}},
	{FileStat: tsm1.FileStat{Path: "000001032-000000002.tsm", Size: 361273384}},
	{FileStat: tsm1.FileStat{Path: "000001040-000000002.tsm", Size: 369431948}},
	{FileStat: tsm1.FileStat{Path: "000001048-000000002.tsm", Size: 416492737}},
	{FileStat: tsm1.FileStat{Path: "000001056-000000002.tsm", Size: 375981616}},
	{FileStat: tsm1.FileStat{Path: "000001064-000000002.tsm", Size: 431468240}},
	{FileStat: tsm1.FileStat{Path: "000001072-000000002.tsm", Size: 369491691}},
	{FileStat: tsm1.FileStat{Path: "000001080-000000002.tsm", Size: 373021342}},
	{FileStat: tsm1.FileStat{Path: "000001088-000000002.tsm", Size: 416823576}},
	{FileStat: tsm1.FileStat{Path: "000001096-000000002.tsm", Size: 373742280}},
	{FileStat: tsm1.FileStat{Path: "000001104-000000002.tsm", Size: 399517201}},
	{FileStat: tsm1.FileStat{Path: "000001112-000000002.tsm", Size: 391644381}},
	{FileStat: tsm1.FileStat{Path: "000001120-000000002.tsm", Size: 383548929}},
	{FileStat: tsm1.FileStat{Path: "000001152-000000002.tsm", Size: 388052321}},
	{FileStat: tsm1.FileStat{Path: "000001160-000000002.tsm", Size: 357873567}},
	{FileStat: tsm1.FileStat{Path: "000001168-000000002.tsm", Size: 370475893}},
	{FileStat: tsm1.FileStat{Path: "000001176-000000002.tsm", Size: 413817543}},
	{FileStat: tsm1.FileStat{Path: "000001184-000000002.tsm", Size: 380255612}},
	{FileStat: tsm1.FileStat{Path: "000001192-000000002.tsm", Size: 368980532}},
	{FileStat: tsm1.FileStat{Path: "000001200-000000002.tsm", Size: 380643043}},
	{FileStat: tsm1.FileStat{Path: "000001208-000000002.tsm", Size: 381010314}},
	{FileStat: tsm1.FileStat{Path: "000001216-000000002.tsm", Size: 423721055}},
	{FileStat: tsm1.FileStat{Path: "000001224-000000002.tsm", Size: 398059420}},
	{FileStat: tsm1.FileStat{Path: "000001232-000000002.tsm", Size: 371391271}},
	{FileStat: tsm1.FileStat{Path: "000001240-000000002.tsm", Size: 398056042}},
	{FileStat: tsm1.FileStat{Path: "000001256-000000002.tsm", Size: 432563948}},
	{FileStat: tsm1.FileStat{Path: "000001264-000000002.tsm", Size: 345480098}},
	{FileStat: tsm1.FileStat{Path: "000001272-000000002.tsm", Size: 370574390}},
	{FileStat: tsm1.FileStat{Path: "000001280-000000002.tsm", Size: 416278264}},
	{FileStat: tsm1.FileStat{Path: "000001312-000000003.tsm", Size: 1394667040}},
	{FileStat: tsm1.FileStat{Path: "000001320-000000002.tsm", Size: 367608354}},
	{FileStat: tsm1.FileStat{Path: "000001328-000000002.tsm", Size: 373211730}},
	{FileStat: tsm1.FileStat{Path: "000001336-000000002.tsm", Size: 404733019}},
	{FileStat: tsm1.FileStat{Path: "000001344-000000002.tsm", Size: 400588350}},
	{FileStat: tsm1.FileStat{Path: "000001352-000000002.tsm", Size: 383289128}},
	{FileStat: tsm1.FileStat{Path: "000001361-000000002.tsm", Size: 419261655}},
	{FileStat: tsm1.FileStat{Path: "000001369-000000002.tsm", Size: 390113125}},
	{FileStat: tsm1.FileStat{Path: "000001377-000000002.tsm", Size: 395545060}},
	{FileStat: tsm1.FileStat{Path: "000001385-000000002.tsm", Size: 431513492}},
	{FileStat: tsm1.FileStat{Path: "000001393-000000002.tsm", Size: 357158485}},
	{FileStat: tsm1.FileStat{Path: "000001401-000000002.tsm", Size: 373396498}},
	{FileStat: tsm1.FileStat{Path: "000001409-000000002.tsm", Size: 406703651}},
	{FileStat: tsm1.FileStat{Path: "000001417-000000002.tsm", Size: 353628259}},
	{FileStat: tsm1.FileStat{Path: "000001425-000000002.tsm", Size: 353945363}},
	{FileStat: tsm1.FileStat{Path: "000001426-000000001.tsm", Size: 51690087}},
	{FileStat: tsm1.FileStat{Path: "000001427-000000001.tsm", Size: 55627805}},
	{FileStat: tsm1.FileStat{Path: "000001428-000000001.tsm", Size: 92359042}},
	{FileStat: tsm1.FileStat{Path: "000001429-000000001.tsm", Size: 94034971}},
	{FileStat: tsm1.FileStat{Path: "000001430-000000001.tsm", Size: 91587157}},
	{FileStat: tsm1.FileStat{Path: "000001431-000000001.tsm", Size: 65765013}},
	{FileStat: tsm1.FileStat{Path: "000001432-000000001.tsm", Size: 53714705}},
	{FileStat: tsm1.FileStat{Path: "000001433-000000001.tsm", Size: 45482279}},
	{FileStat: tsm1.FileStat{Path: "000004650-000000004.tsm", Size: 2147989397}},
}

var p4Shard6165Files = []tsm1.ExtFileStat{
	{FileStat: tsm1.FileStat{Path: "000000672-000000003.tsm", Size: 1417422245}},
	{FileStat: tsm1.FileStat{Path: "000000704-000000003.tsm", Size: 1395660558}},
	{FileStat: tsm1.FileStat{Path: "000000760-000000003.tsm", Size: 1422252445}},
	{FileStat: tsm1.FileStat{Path: "000000792-000000003.tsm", Size: 1509170528}},
	{FileStat: tsm1.FileStat{Path: "000000808-000000002.tsm", Size: 386111741}},
	{FileStat: tsm1.FileStat{Path: "000000816-000000002.tsm", Size: 465209132}},
	{FileStat: tsm1.FileStat{Path: "000000824-000000002.tsm", Size: 359423091}},
	{FileStat: tsm1.FileStat{Path: "000000832-000000002.tsm", Size: 393764986}},
	{FileStat: tsm1.FileStat{Path: "000000856-000000002.tsm", Size: 377636761}},
	{FileStat: tsm1.FileStat{Path: "000000864-000000002.tsm", Size: 427418323}},
	{FileStat: tsm1.FileStat{Path: "000000872-000000002.tsm", Size: 334321990}},
	{FileStat: tsm1.FileStat{Path: "000000880-000000002.tsm", Size: 392461907}},
	{FileStat: tsm1.FileStat{Path: "000000888-000000002.tsm", Size: 369064583}},
	{FileStat: tsm1.FileStat{Path: "000000896-000000002.tsm", Size: 426594825}},
	{FileStat: tsm1.FileStat{Path: "000000904-000000002.tsm", Size: 371474968}},
	{FileStat: tsm1.FileStat{Path: "000000912-000000002.tsm", Size: 502441654}},
	{FileStat: tsm1.FileStat{Path: "000000920-000000002.tsm", Size: 355395037}},
	{FileStat: tsm1.FileStat{Path: "000000928-000000002.tsm", Size: 348750472}},
	{FileStat: tsm1.FileStat{Path: "000000936-000000002.tsm", Size: 410735906}},
	{FileStat: tsm1.FileStat{Path: "000000944-000000002.tsm", Size: 399868044}},
	{FileStat: tsm1.FileStat{Path: "000000952-000000002.tsm", Size: 380381997}},
	{FileStat: tsm1.FileStat{Path: "000000960-000000002.tsm", Size: 387208074}},
	{FileStat: tsm1.FileStat{Path: "000000968-000000002.tsm", Size: 399933180}},
	{FileStat: tsm1.FileStat{Path: "000000976-000000002.tsm", Size: 347923046}},
	{FileStat: tsm1.FileStat{Path: "000000984-000000002.tsm", Size: 368692511}},
	{FileStat: tsm1.FileStat{Path: "000000992-000000002.tsm", Size: 396793328}},
	{FileStat: tsm1.FileStat{Path: "000001000-000000002.tsm", Size: 366165427}},
	{FileStat: tsm1.FileStat{Path: "000001008-000000002.tsm", Size: 362511920}},
	{FileStat: tsm1.FileStat{Path: "000001016-000000002.tsm", Size: 374042165}},
	{FileStat: tsm1.FileStat{Path: "000001024-000000002.tsm", Size: 345291314}},
	{FileStat: tsm1.FileStat{Path: "000001032-000000002.tsm", Size: 367658352}},
	{FileStat: tsm1.FileStat{Path: "000001040-000000002.tsm", Size: 369994657}},
	{FileStat: tsm1.FileStat{Path: "000001048-000000002.tsm", Size: 364563655}},
	{FileStat: tsm1.FileStat{Path: "000001056-000000002.tsm", Size: 385809355}},
	{FileStat: tsm1.FileStat{Path: "000001064-000000002.tsm", Size: 369554099}},
	{FileStat: tsm1.FileStat{Path: "000001072-000000002.tsm", Size: 401544997}},
	{FileStat: tsm1.FileStat{Path: "000001080-000000002.tsm", Size: 369289865}},
	{FileStat: tsm1.FileStat{Path: "000001088-000000002.tsm", Size: 406286985}},
	{FileStat: tsm1.FileStat{Path: "000001096-000000002.tsm", Size: 400457935}},
	{FileStat: tsm1.FileStat{Path: "000001104-000000002.tsm", Size: 375341350}},
	{FileStat: tsm1.FileStat{Path: "000001112-000000002.tsm", Size: 385863407}},
	{FileStat: tsm1.FileStat{Path: "000001120-000000002.tsm", Size: 372037362}},
	{FileStat: tsm1.FileStat{Path: "000001128-000000002.tsm", Size: 421128784}},
	{FileStat: tsm1.FileStat{Path: "000001136-000000002.tsm", Size: 402098154}},
	{FileStat: tsm1.FileStat{Path: "000001144-000000002.tsm", Size: 405079389}},
	{FileStat: tsm1.FileStat{Path: "000001152-000000002.tsm", Size: 446994013}},
	{FileStat: tsm1.FileStat{Path: "000001160-000000002.tsm", Size: 387072638}},
	{FileStat: tsm1.FileStat{Path: "000001161-000000001.tsm", Size: 64602018}},
	{FileStat: tsm1.FileStat{Path: "000001162-000000001.tsm", Size: 62664900}},
	{FileStat: tsm1.FileStat{Path: "000001163-000000001.tsm", Size: 65525094}},
	{FileStat: tsm1.FileStat{Path: "000001164-000000001.tsm", Size: 63494992}},
	{FileStat: tsm1.FileStat{Path: "000001165-000000001.tsm", Size: 63840194}},
	{FileStat: tsm1.FileStat{Path: "000001166-000000001.tsm", Size: 89277194}},
	{FileStat: tsm1.FileStat{Path: "000001167-000000001.tsm", Size: 64861893}},
	{FileStat: tsm1.FileStat{Path: "000001168-000000001.tsm", Size: 64352905}},
	{FileStat: tsm1.FileStat{Path: "000001696-000000004.tsm", Size: 2147644001}},
}

var p1Shard6156Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001433-000000001.tsm",
			"000001434-000000001.tsm",
			"000001435-000000001.tsm",
			"000001436-000000001.tsm",
			"000001437-000000001.tsm",
			"000001438-000000001.tsm",
			"000001439-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001416-000000002.tsm",
			"000001424-000000002.tsm",
			"000001432-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001344-000000003.tsm",
			"000001376-000000003.tsm",
			"000001408-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000005010-000000027.tsm",
		}},
	},
}

var p1Shard6159Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002648-000000002.tsm",
			"000002656-000000002.tsm",
			"000002664-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001048-000000003.tsm",
			"000001080-000000003.tsm",
			"000001112-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002608-000000003.tsm",
			"000002640-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001600-000000004.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002672-000000018.tsm",
		}},
	},
}

var p1Shard6161Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002114-000000001.tsm",
			"000002115-000000001.tsm",
			"000002116-000000001.tsm",
			"000002119-000000001.tsm",
			"000002120-000000001.tsm",
			"000002121-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001625-000000002.tsm",
			"000001633-000000002.tsm",
			"000001641-000000002.tsm",
			"000001649-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001657-000000002.tsm",
			"000001665-000000002.tsm",
			"000001673-000000002.tsm",
			"000001681-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001689-000000002.tsm",
			"000001697-000000002.tsm",
			"000001705-000000002.tsm",
			"000001713-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001729-000000002.tsm",
			"000001737-000000002.tsm",
			"000001745-000000002.tsm",
			"000001753-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001761-000000002.tsm",
			"000001769-000000002.tsm",
			"000001777-000000002.tsm",
			"000001785-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001793-000000002.tsm",
			"000001801-000000002.tsm",
			"000001809-000000002.tsm",
			"000001817-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001825-000000002.tsm",
			"000001833-000000002.tsm",
			"000001841-000000002.tsm",
			"000001849-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001865-000000002.tsm",
			"000001873-000000002.tsm",
			"000001881-000000002.tsm",
			"000001889-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001897-000000002.tsm",
			"000001905-000000002.tsm",
			"000001913-000000002.tsm",
			"000001921-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001929-000000002.tsm",
			"000001937-000000002.tsm",
			"000001945-000000002.tsm",
			"000001953-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001961-000000002.tsm",
			"000001969-000000002.tsm",
			"000001977-000000002.tsm",
			"000001985-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001993-000000002.tsm",
			"000002001-000000002.tsm",
			"000002009-000000002.tsm",
			"000002017-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002025-000000002.tsm",
			"000002033-000000002.tsm",
			"000002041-000000002.tsm",
			"000002049-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002057-000000002.tsm",
			"000002065-000000002.tsm",
			"000002073-000000002.tsm",
			"000002081-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002089-000000002.tsm",
			"000002097-000000002.tsm",
			"000002105-000000002.tsm",
			"000002113-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001545-000000003.tsm",
			"000001585-000000003.tsm",
			"000001617-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004402-000000004.tsm",
		}},
	},
}

var p1Shard6162Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001681-000000001.tsm",
			"000001682-000000001.tsm",
			"000001683-000000001.tsm",
			"000001684-000000001.tsm",
			"000001685-000000001.tsm",
			"000001686-000000001.tsm",
			"000001687-000000001.tsm",
			"000001688-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001360-000000002.tsm",
			"000001368-000000002.tsm",
			"000001376-000000002.tsm",
			"000001384-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001392-000000002.tsm",
			"000001400-000000002.tsm",
			"000001408-000000002.tsm",
			"000001416-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001424-000000002.tsm",
			"000001432-000000002.tsm",
			"000001440-000000002.tsm",
			"000001448-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001456-000000002.tsm",
			"000001464-000000002.tsm",
			"000001472-000000002.tsm",
			"000001480-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001488-000000002.tsm",
			"000001496-000000002.tsm",
			"000001504-000000002.tsm",
			"000001512-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001520-000000002.tsm",
			"000001528-000000002.tsm",
			"000001536-000000002.tsm",
			"000001544-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001552-000000002.tsm",
			"000001560-000000002.tsm",
			"000001568-000000002.tsm",
			"000001576-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001584-000000002.tsm",
			"000001592-000000002.tsm",
			"000001600-000000002.tsm",
			"000001608-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001616-000000002.tsm",
			"000001624-000000002.tsm",
			"000001632-000000002.tsm",
			"000001640-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001648-000000002.tsm",
			"000001656-000000002.tsm",
			"000001664-000000002.tsm",
			"000001672-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001680-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001056-000000003.tsm",
			"000001088-000000003.tsm",
			"000001120-000000003.tsm",
			"000001152-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001184-000000003.tsm",
			"000001216-000000003.tsm",
			"000001248-000000003.tsm",
			"000001280-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001312-000000003.tsm",
			"000001352-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004401-000000004.tsm",
		}},
	},
}

var p1Shard6164Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000801-000000001.tsm",
			"000000802-000000001.tsm",
			"000000803-000000001.tsm",
			"000000804-000000001.tsm",
			"000000805-000000001.tsm",
			"000000806-000000001.tsm",
			"000000807-000000001.tsm",
			"000000808-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000680-000000002.tsm",
			"000000688-000000002.tsm",
			"000000696-000000002.tsm",
			"000000704-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000712-000000002.tsm",
			"000000720-000000002.tsm",
			"000000728-000000002.tsm",
			"000000736-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000744-000000002.tsm",
			"000000752-000000002.tsm",
			"000000760-000000002.tsm",
			"000000768-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000776-000000002.tsm",
			"000000784-000000002.tsm",
			"000000792-000000002.tsm",
			"000000800-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000504-000000003.tsm",
			"000000536-000000003.tsm",
			"000000568-000000003.tsm",
			"000000600-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000632-000000003.tsm",
			"000000672-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000944-000000004.tsm",
		}},
	},
}

var p2Shard6147Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002138-000000001.tsm",
			"000002139-000000001.tsm",
			"000002140-000000001.tsm",
			"000002141-000000001.tsm",
			"000002142-000000001.tsm",
			"000002143-000000001.tsm",
			"000002144-000000001.tsm",
			"000002145-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002097-000000002.tsm",
			"000002105-000000002.tsm",
			"000002113-000000002.tsm",
			"000002121-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002129-000000002.tsm",
			"000002137-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002089-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004707-000000032.tsm",
		}},
	},
}

var p2Shard6158Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003689-000000001.tsm",
			"000003690-000000001.tsm",
			"000003691-000000001.tsm",
			"000003692-000000001.tsm",
			"000003693-000000001.tsm",
			"000003694-000000001.tsm",
			"000003695-000000001.tsm",
			"000003696-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003120-000000002.tsm",
			"000003128-000000002.tsm",
			"000003136-000000002.tsm",
			"000003144-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003152-000000002.tsm",
			"000003160-000000002.tsm",
			"000003168-000000002.tsm",
			"000003176-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003184-000000002.tsm",
			"000003192-000000002.tsm",
			"000003200-000000002.tsm",
			"000003208-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003216-000000002.tsm",
			"000003224-000000002.tsm",
			"000003232-000000002.tsm",
			"000003240-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003248-000000002.tsm",
			"000003256-000000002.tsm",
			"000003264-000000002.tsm",
			"000003272-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003280-000000002.tsm",
			"000003288-000000002.tsm",
			"000003296-000000002.tsm",
			"000003304-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003312-000000002.tsm",
			"000003320-000000002.tsm",
			"000003328-000000002.tsm",
			"000003336-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003344-000000002.tsm",
			"000003352-000000002.tsm",
			"000003360-000000002.tsm",
			"000003368-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003376-000000002.tsm",
			"000003384-000000002.tsm",
			"000003392-000000002.tsm",
			"000003400-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003408-000000002.tsm",
			"000003416-000000002.tsm",
			"000003424-000000002.tsm",
			"000003432-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003440-000000002.tsm",
			"000003448-000000002.tsm",
			"000003456-000000002.tsm",
			"000003464-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003472-000000002.tsm",
			"000003480-000000002.tsm",
			"000003488-000000002.tsm",
			"000003496-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003504-000000002.tsm",
			"000003512-000000002.tsm",
			"000003520-000000002.tsm",
			"000003528-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003536-000000002.tsm",
			"000003544-000000002.tsm",
			"000003552-000000002.tsm",
			"000003560-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003592-000000002.tsm",
			"000003600-000000002.tsm",
			"000003608-000000002.tsm",
			"000003616-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003624-000000002.tsm",
			"000003632-000000002.tsm",
			"000003640-000000002.tsm",
			"000003648-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003656-000000002.tsm",
			"000003664-000000002.tsm",
			"000003672-000000002.tsm",
			"000003680-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003688-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002984-000000003.tsm",
			"000003016-000000003.tsm",
			"000003048-000000003.tsm",
			"000003080-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003112-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004508-000000035.tsm",
		}},
	},
}

var p2Shard6159Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002665-000000001.tsm",
			"000002666-000000001.tsm",
			"000002667-000000001.tsm",
			"000002668-000000001.tsm",
			"000002669-000000001.tsm",
			"000002670-000000001.tsm",
			"000002671-000000001.tsm",
			"000002672-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002648-000000002.tsm",
			"000002656-000000002.tsm",
			"000002664-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002608-000000003.tsm",
			"000002640-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004396-000000047.tsm",
		}},
	},
}

var p2Shard6160Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002753-000000003.tsm",
			"000002785-000000003.tsm",
			"000002817-000000003.tsm",
			"000002849-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000002881-000000003.tsm",
			"000002913-000000003.tsm",
			"000002945-000000003.tsm",
			"000002977-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003009-000000003.tsm",
			"000003041-000000003.tsm",
			"000003073-000000003.tsm",
			"000003105-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003137-000000003.tsm",
			"000003169-000000003.tsm",
			"000003201-000000003.tsm",
			"000003233-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003265-000000003.tsm",
			"000003297-000000003.tsm",
			"000003329-000000003.tsm",
			"000003361-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003393-000000003.tsm",
			"000003425-000000003.tsm",
			"000003465-000000003.tsm",
			"000003497-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003529-000000003.tsm",
			"000003561-000000003.tsm",
			"000003593-000000003.tsm",
			"000003625-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003657-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003665-000000004.tsm",
		}},
	},
}

var p2Shard6164Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000512-000000003.tsm",
			"000000544-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000576-000000004.tsm",
		}},
	},
}

var p3Shard6156Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003313-000000001.tsm",
			"000003314-000000001.tsm",
			"000003315-000000001.tsm",
			"000003316-000000001.tsm",
			"000003317-000000001.tsm",
			"000003318-000000001.tsm",
			"000003319-000000001.tsm",
			"000003320-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003256-000000002.tsm",
			"000003264-000000002.tsm",
			"000003272-000000002.tsm",
			"000003280-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003288-000000002.tsm",
			"000003296-000000002.tsm",
			"000003304-000000002.tsm",
			"000003312-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003120-000000003.tsm",
			"000003152-000000003.tsm",
			"000003184-000000003.tsm",
			"000003216-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003248-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004980-000000099.tsm",
		}},
	},
}

var p3Shard6159Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001913-000000001.tsm",
			"000001914-000000001.tsm",
			"000001915-000000001.tsm",
			"000001916-000000001.tsm",
			"000001917-000000001.tsm",
			"000001918-000000001.tsm",
			"000001919-000000001.tsm",
			"000001920-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001768-000000002.tsm",
			"000001776-000000002.tsm",
			"000001784-000000002.tsm",
			"000001792-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001800-000000002.tsm",
			"000001808-000000002.tsm",
			"000001816-000000002.tsm",
			"000001824-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001832-000000002.tsm",
			"000001840-000000002.tsm",
			"000001848-000000002.tsm",
			"000001856-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001864-000000002.tsm",
			"000001872-000000002.tsm",
			"000001880-000000002.tsm",
			"000001888-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001896-000000002.tsm",
			"000001904-000000002.tsm",
			"000001912-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001664-000000003.tsm",
			"000001696-000000003.tsm",
			"000001728-000000003.tsm",
			"000001760-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000003712-000000007.tsm",
		}},
	},
}

var p3Shard6161Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001361-000000002.tsm",
			"000001369-000000002.tsm",
			"000001377-000000002.tsm",
			"000001385-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001393-000000002.tsm",
			"000001401-000000002.tsm",
			"000001409-000000002.tsm",
			"000001417-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001425-000000002.tsm",
			"000001433-000000002.tsm",
			"000001441-000000002.tsm",
			"000001449-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001457-000000002.tsm",
			"000001465-000000002.tsm",
			"000001473-000000002.tsm",
			"000001481-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001489-000000002.tsm",
			"000001497-000000002.tsm",
			"000001505-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001225-000000003.tsm",
			"000001257-000000003.tsm",
			"000001289-000000003.tsm",
			"000001321-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001353-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001513-000000004.tsm",
		}},
	},
}

var p3Shard6162Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000928-000000002.tsm",
			"000000936-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000800-000000003.tsm",
			"000000856-000000003.tsm",
			"000000888-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000920-000000007.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001064-000000004.tsm",
		}},
	},
}

var p3Shard6163Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000361-000000001.tsm",
			"000000362-000000001.tsm",
			"000000363-000000001.tsm",
			"000000364-000000001.tsm",
			"000000365-000000001.tsm",
			"000000366-000000001.tsm",
			"000000367-000000001.tsm",
			"000000368-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000344-000000002.tsm",
			"000000352-000000002.tsm",
			"000000360-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000005416-000000004.tsm",
		}},
	},
}

var p3Shard6166Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000264-000000002.tsm",
			"000000272-000000002.tsm",
			"000000280-000000002.tsm",
			"000000288-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000296-000000002.tsm",
			"000000304-000000002.tsm",
			"000000312-000000002.tsm",
			"000000320-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000032-000000003.tsm",
			"000000064-000000003.tsm",
			"000000096-000000003.tsm",
			"000000128-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000256-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000352-000000004.tsm",
		}},
	},
}

var p4Shard6159Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001353-000000001.tsm",
			"000001354-000000001.tsm",
			"000001355-000000001.tsm",
			"000001356-000000001.tsm",
			"000001357-000000001.tsm",
			"000001358-000000001.tsm",
			"000001359-000000001.tsm",
			"000001360-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001328-000000002.tsm",
			"000001336-000000002.tsm",
			"000001344-000000002.tsm",
			"000001352-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004275-000000004.tsm",
		}},
	},
}

var p4Shard6161Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001562-000000001.tsm",
			"000001563-000000001.tsm",
			"000001564-000000001.tsm",
			"000001565-000000001.tsm",
			"000001566-000000001.tsm",
			"000001567-000000001.tsm",
			"000001568-000000001.tsm",
			"000001569-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001553-000000002.tsm",
			"000001561-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001545-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004025-000000004.tsm",
		}},
	},
}

var p4Shard6162Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001426-000000001.tsm",
			"000001427-000000001.tsm",
			"000001428-000000001.tsm",
			"000001429-000000001.tsm",
			"000001430-000000001.tsm",
			"000001431-000000001.tsm",
			"000001432-000000001.tsm",
			"000001433-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001000-000000002.tsm",
			"000001008-000000002.tsm",
			"000001016-000000002.tsm",
			"000001024-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001032-000000002.tsm",
			"000001040-000000002.tsm",
			"000001048-000000002.tsm",
			"000001056-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001064-000000002.tsm",
			"000001072-000000002.tsm",
			"000001080-000000002.tsm",
			"000001088-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001096-000000002.tsm",
			"000001104-000000002.tsm",
			"000001112-000000002.tsm",
			"000001120-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001152-000000002.tsm",
			"000001160-000000002.tsm",
			"000001168-000000002.tsm",
			"000001176-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001184-000000002.tsm",
			"000001192-000000002.tsm",
			"000001200-000000002.tsm",
			"000001208-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001216-000000002.tsm",
			"000001224-000000002.tsm",
			"000001232-000000002.tsm",
			"000001240-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001256-000000002.tsm",
			"000001264-000000002.tsm",
			"000001272-000000002.tsm",
			"000001280-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001320-000000002.tsm",
			"000001328-000000002.tsm",
			"000001336-000000002.tsm",
			"000001344-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001352-000000002.tsm",
			"000001361-000000002.tsm",
			"000001369-000000002.tsm",
			"000001377-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001385-000000002.tsm",
			"000001393-000000002.tsm",
			"000001401-000000002.tsm",
			"000001409-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001417-000000002.tsm",
			"000001425-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000992-000000003.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001312-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000004650-000000004.tsm",
		}},
	},
}

var p4Shard6165Expected = TestLevelResults{
	level1Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001161-000000001.tsm",
			"000001162-000000001.tsm",
			"000001163-000000001.tsm",
			"000001164-000000001.tsm",
			"000001165-000000001.tsm",
			"000001166-000000001.tsm",
			"000001167-000000001.tsm",
			"000001168-000000001.tsm",
		}},
	},
	level2Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000808-000000002.tsm",
			"000000816-000000002.tsm",
			"000000824-000000002.tsm",
			"000000832-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000856-000000002.tsm",
			"000000864-000000002.tsm",
			"000000872-000000002.tsm",
			"000000880-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000888-000000002.tsm",
			"000000896-000000002.tsm",
			"000000904-000000002.tsm",
			"000000912-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000920-000000002.tsm",
			"000000928-000000002.tsm",
			"000000936-000000002.tsm",
			"000000944-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000952-000000002.tsm",
			"000000960-000000002.tsm",
			"000000968-000000002.tsm",
			"000000976-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000984-000000002.tsm",
			"000000992-000000002.tsm",
			"000001000-000000002.tsm",
			"000001008-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001016-000000002.tsm",
			"000001024-000000002.tsm",
			"000001032-000000002.tsm",
			"000001040-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001048-000000002.tsm",
			"000001056-000000002.tsm",
			"000001064-000000002.tsm",
			"000001072-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001080-000000002.tsm",
			"000001088-000000002.tsm",
			"000001096-000000002.tsm",
			"000001104-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001112-000000002.tsm",
			"000001120-000000002.tsm",
			"000001128-000000002.tsm",
			"000001136-000000002.tsm",
		}},
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001144-000000002.tsm",
			"000001152-000000002.tsm",
			"000001160-000000002.tsm",
		}},
	},
	level3Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000000672-000000003.tsm",
			"000000704-000000003.tsm",
			"000000760-000000003.tsm",
			"000000792-000000003.tsm",
		}},
	},
	level4Groups: []tsm1.PlannedCompactionGroup{},
	level5Groups: []tsm1.PlannedCompactionGroup{
		{PointsPerBlock: 1000, Group: tsm1.CompactionGroup{
			"000001696-000000004.tsm",
		}},
	},
}

func TestProductionShardCompactions(t *testing.T) {
	e, err := NewEngine(tsdb.InmemIndexName)
	require.NoError(t, err)
	defer e.Close()

	tests := []struct {
		name     string
		files    []tsm1.ExtFileStat
		expected TestLevelResults
	}{
		{name: "p1/shard_6156", files: p1Shard6156Files, expected: p1Shard6156Expected},
		{name: "p1/shard_6159", files: p1Shard6159Files, expected: p1Shard6159Expected},
		{name: "p1/shard_6161", files: p1Shard6161Files, expected: p1Shard6161Expected},
		{name: "p1/shard_6162", files: p1Shard6162Files, expected: p1Shard6162Expected},
		{name: "p1/shard_6164", files: p1Shard6164Files, expected: p1Shard6164Expected},
		{name: "p2/shard_6147", files: p2Shard6147Files, expected: p2Shard6147Expected},
		{name: "p2/shard_6158", files: p2Shard6158Files, expected: p2Shard6158Expected},
		{name: "p2/shard_6159", files: p2Shard6159Files, expected: p2Shard6159Expected},
		{name: "p2/shard_6160", files: p2Shard6160Files, expected: p2Shard6160Expected},
		{name: "p2/shard_6164", files: p2Shard6164Files, expected: p2Shard6164Expected},
		{name: "p3/shard_6156", files: p3Shard6156Files, expected: p3Shard6156Expected},
		{name: "p3/shard_6159", files: p3Shard6159Files, expected: p3Shard6159Expected},
		{name: "p3/shard_6161", files: p3Shard6161Files, expected: p3Shard6161Expected},
		{name: "p3/shard_6162", files: p3Shard6162Files, expected: p3Shard6162Expected},
		{name: "p3/shard_6163", files: p3Shard6163Files, expected: p3Shard6163Expected},
		{name: "p3/shard_6166", files: p3Shard6166Files, expected: p3Shard6166Expected},
		{name: "p4/shard_6159", files: p4Shard6159Files, expected: p4Shard6159Expected},
		{name: "p4/shard_6161", files: p4Shard6161Files, expected: p4Shard6161Expected},
		{name: "p4/shard_6162", files: p4Shard6162Files, expected: p4Shard6162Expected},
		{name: "p4/shard_6165", files: p4Shard6165Files, expected: p4Shard6165Expected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ffs := newFakeFileStore(
				withExtFileStats(t, test.files),
				withDefaultBlockCount(tsdb.DefaultMaxPointsPerBlock),
			)
			cp := tsm1.NewDefaultPlanner(ffs, -1)

			e.MaxPointsPerBlock = tsdb.DefaultMaxPointsPerBlock
			e.CompactionPlan = cp
			e.Compactor.FileStore = ffs

			// Set scheduler depth for lower level groups.
			mockGroupLen := 5
			e.Scheduler.SetDepth(1, mockGroupLen)
			e.Scheduler.SetDepth(2, mockGroupLen)
			atomic.StoreInt64(&e.Stats.TSMCompactionsActive[0], int64(mockGroupLen))
			atomic.StoreInt64(&e.Stats.TSMCompactionsActive[1], int64(mockGroupLen))

			level1, level2, level3, level4, level5 := e.PlanCompactions()

			actual := TestLevelResults{
				level1Groups: level1,
				level2Groups: level2,
				level3Groups: level3,
				level4Groups: level4,
				level5Groups: level5,
			}

			// Validate adjacency invariant.
			allFiles := make([]string, len(test.files))
			for i, f := range test.files {
				allFiles[i] = f.Path
			}
			assert.NoError(t, ValidateCompactionProperties(
				allFiles, actual, AdjacentFileProperty,
			))

			// Validate expected groupings.
			assert.Equal(t, test.expected, actual)

			// Log summary.
			planned := countPlanned(level1, level2, level3, level4, level5)
			t.Logf("total: %d, planned: %d, unplanned: %d",
				len(test.files), planned, len(test.files)-planned)

			e.ReleaseCompactionPlans(level1, level2, level3, level4, level5)
			require.Zero(t, cp.InUseCount(), "some TSM files were not released")
		})
	}
}

func countPlanned(groups ...[]tsm1.PlannedCompactionGroup) int {
	n := 0
	for _, gs := range groups {
		for _, g := range gs {
			n += len(g.Group)
		}
	}
	return n
}
