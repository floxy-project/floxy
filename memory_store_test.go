package floxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore_ReplaceInJoinWaitForCreatesMissingState(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- store.ReplaceInJoinWaitFor(ctx, 1, "collect", "cond#check", "item2-then")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ReplaceInJoinWaitFor deadlocked")
	}

	joinState, err := store.GetJoinState(ctx, 1, "collect")
	require.NoError(t, err)
	assert.Equal(t, []string{"item2-then"}, joinState.WaitingFor)
}

func TestMemoryStore_JoinStateDoesNotAliasWaitFor(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	waitFor := []string{"cond#check", "item1"}
	require.NoError(t, store.CreateJoinState(ctx, 1, "collect", waitFor, JoinStrategyAll))
	require.NoError(t, store.ReplaceInJoinWaitFor(ctx, 1, "collect", "cond#check", "item2-then"))

	assert.Equal(t, []string{"cond#check", "item1"}, waitFor)
}
