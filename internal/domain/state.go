package domain

import "time"

type PinSource string

const (
	PinNone     PinSource = ""
	PinBaseline PinSource = "baseline"
	PinUser     PinSource = "user"
)

type BookmarkState struct {
	ID                ID         `json:"id"`
	KnownCollection   ID         `json:"known_collection"`
	KnownTags         []string   `json:"known_tags"`
	PinSource         PinSource  `json:"pin_source"`
	PinnedAt          *time.Time `json:"pinned_at,omitempty"`
	InputHash         string     `json:"input_hash"`
	TaxonomyVersion   int        `json:"taxonomy_version"`
	Model             string     `json:"model"`
	Status            string     `json:"status"`
	PendingCollection *ID        `json:"pending_collection,omitempty"`
	PendingTags       []string   `json:"pending_tags,omitempty"`
	FetchAttempts     int        `json:"fetch_attempts"`
	ProcessedAt       *time.Time `json:"processed_at,omitempty"`
	RestoredAt        *time.Time `json:"restored_at,omitempty"`
}

type CollectionChoice struct {
	Key          string `json:"key"`
	ProposedName string `json:"proposed_name,omitempty"`
}

type TagSuggestion struct {
	Name  string   `json:"name"`
	Score *float64 `json:"score,omitempty"`
}

type ModelInfo struct {
	Classifier   string `json:"classifier"`
	Model        string `json:"model"`
	Version      string `json:"version"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
}

type Decision struct {
	Collection CollectionChoice `json:"collection"`
	Tags       []TagSuggestion  `json:"tags"`
	Confidence float64          `json:"confidence"`
	Reasoning  string           `json:"reasoning,omitempty"`
	Model      ModelInfo        `json:"model"`
}

type DecisionRecord struct {
	JobID              string     `json:"job_id"`
	Bookmark           ID         `json:"bookmark"`
	InputHash          string     `json:"input_hash"`
	ObservedCollection ID         `json:"observed_collection"`
	ObservedTags       []string   `json:"observed_tags"`
	TargetCollection   *ID        `json:"target_collection,omitempty"`
	AddTags            []string   `json:"add_tags"`
	Decision           Decision   `json:"decision"`
	Reason             string     `json:"reason"`
	Result             string     `json:"result"`
	AppliedAt          *time.Time `json:"applied_at,omitempty"`
}

type TaxonomyCollection struct {
	Key         string `json:"key" yaml:"key"`
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Role        string `json:"role" yaml:"role"`
}

type TaxonomyRules struct {
	MinTags       int               `json:"min_tags" yaml:"min_tags"`
	MaxTags       int               `json:"max_tags" yaml:"max_tags"`
	MinConfidence float64           `json:"min_confidence" yaml:"min_confidence"`
	AllowNewTags  bool              `json:"allow_new_tags" yaml:"allow_new_tags"`
	TagAliases    map[string]string `json:"tag_aliases,omitempty" yaml:"tag_aliases"`
	PromptExtra   string            `json:"prompt_extra,omitempty" yaml:"prompt_extra"`
}

type DynamicSettings struct {
	MaxCollections          int     `json:"max_collections" yaml:"max_collections"`
	SampleSize              int     `json:"sample_size" yaml:"sample_size"`
	AutoCreate              bool    `json:"auto_create" yaml:"auto_create"`
	AutoCreateMinConfidence float64 `json:"auto_create_min_confidence" yaml:"auto_create_min_confidence"`
}

type CollectionProposal struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Count       int    `json:"count" yaml:"count"`
}

// Taxonomy modes and statuses.
const (
	ModeFixed   = "fixed"
	ModeDynamic = "dynamic"

	TaxonomyDraft  = "draft"
	TaxonomyActive = "active"
)

type Taxonomy struct {
	ID               string               `json:"id" yaml:"id"`
	Mode             string               `json:"mode" yaml:"mode"`
	Status           string               `json:"status" yaml:"status"`
	Version          int                  `json:"version" yaml:"version"`
	Collections      []TaxonomyCollection `json:"collections" yaml:"collections"`
	Rules            TaxonomyRules        `json:"rules" yaml:"rules"`
	ProvisionMissing bool                 `json:"provision_missing" yaml:"provision_missing"`
	Dynamic          DynamicSettings      `json:"dynamic" yaml:"dynamic"`
	Proposals        []CollectionProposal `json:"proposals,omitempty" yaml:"proposals"`
}

// TaxonomySnapshot is the immutable view of a taxonomy handed to a classifier.
type TaxonomySnapshot struct {
	ID          string               `json:"id"`
	Version     int                  `json:"version"`
	Collections []TaxonomyCollection `json:"collections"`
	Rules       TaxonomyRules        `json:"rules"`
}
