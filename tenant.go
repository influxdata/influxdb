package influxdb

// TenantService is a service that exposes the functionality of the embedded services.
type TenantService interface {
	UserService
	PasswordsService
	UserResourceMappingService
	OrganizationService
	BucketService
}
