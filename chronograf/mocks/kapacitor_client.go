package mocks

// TODO(desa): resolve kapacitor dependency

//var _ kapacitor.KapaClient = &KapaClient{}
//
//// Client is a mock Kapacitor client
//type KapaClient struct {
//	CreateTaskF func(opts client.CreateTaskOptions) (client.Task, error)
//	DeleteTaskF func(link client.Link) error
//	ListTasksF  func(opts *client.ListTasksOptions) ([]client.Task, error)
//	TaskF       func(link client.Link, opts *client.TaskOptions) (client.Task, error)
//	UpdateTaskF func(link client.Link, opts client.UpdateTaskOptions) (client.Task, error)
//}
//
//func (p *KapaClient) CreateTask(opts client.CreateTaskOptions) (client.Task, error) {
//	return p.CreateTaskF(opts)
//}
//
//func (p *KapaClient) DeleteTask(link client.Link) error {
//	return p.DeleteTaskF(link)
//}
//
//func (p *KapaClient) ListTasks(opts *client.ListTasksOptions) ([]client.Task, error) {
//	return p.ListTasksF(opts)
//}
//
//func (p *KapaClient) Task(link client.Link, opts *client.TaskOptions) (client.Task, error) {
//	return p.TaskF(link, opts)
//}
//
//func (p *KapaClient) UpdateTask(link client.Link, opts client.UpdateTaskOptions) (client.Task, error) {
//	return p.UpdateTaskF(link, opts)
//}
