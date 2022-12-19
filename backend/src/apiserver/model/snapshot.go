package model

// The type of a resource object.
type SnapshotClass string

type Snapshot struct {
	SnapshotUUID  string `gorm:"column:UUID; not null; primary_key"`
	SnapshotName  string `gorm:"column:Name; not null;"`
	SnapshotClass string `gorm:"column:Class; not null"`
	pvcName       string `gorm:"column:pvcName; not null"`
}
