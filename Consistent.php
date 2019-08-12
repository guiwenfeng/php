<?php
/**
 * 一致性哈希
 * User: gwf
 * Date: 2019/8/12
 * Time: 15:08
 */
namespace Consistent;

/**
 * A simple consistent hashing implementation with pluggable hash algorithms.
 */
class Consistent {

    /**
     * @var int The number of positions to hash each node to.
     */
    private $replicas = 64;

    /**
     * @var object The hash algorithm.
     */
    private $hasher;

    /**
     * @var int Number of nodes.
     */
    private $nodeCount = 0;

    /**
     * @var array Map of positions to nodes.
     */
    private $positionToNode = [];

    /**
     * @var array Map of nodes to lists of positions that node is hashed to.
     */
    private $nodeToPositions = [];

    /**
     * @var bool Whether the map of positions to nodes is already sorted.
     */
    private $positionToNodeSorted = false;

    /**
     * Consistent constructor.
     * @param HasherInterface|null $hasher
     * @param int $replicas Amount of positions to hash each node to.
     */
    public function __construct(HasherInterface $hasher = null, $replicas = 0)
    {
        $this->hasher = $hasher ? $hasher : new Crc32Hasher();
        if (!empty($replicas)) {
            $this->replicas = $replicas;
        }
    }

    /**
     * Add a node.
     * @param $node
     * @param int $weight
     */
    public function addNode($node, $weight = 1)
    {
        if (isset($this->nodeToPositions[$node])) {
            throw new \Exception("Node '$node' already exists.");
        }

        $this->nodeToPositions[$node] = [];

        // hash the node into multiple positions
        for ($i = 0; $i < round($this->replicas * $weight); ++$i) {
            $position = $this->hasher->hash($node.$i);
            $this->positionToNode[$position] = $node;
            $this->nodeToPositions[$node][] = $position;
        }

        $this->positionToNodeSorted = false;
        ++$this->nodeCount;

        return $this;
    }

    /**
     * Add a list of nodes.
     * @param array $nodes
     * @param int $weight
     */
    public function addNodes($nodes, $weight = 1)
    {
        foreach ($nodes as $node) {
            $this->addNode($node, $weight);
        }

        return $this;
    }

    /**
     * Remove a node.
     * @param $node
     */
    public function removeNode($node)
    {
        if (!isset($this->nodeToPositions[$node])) {
            throw new \Exception("Node '$node' does not exist.");
        }

        foreach ($this->nodeToPositions[$node] as $position) {
            unset($this->positionToNode[$position]);
        }

        unset($this->nodeToPositions[$node]);

        --$this->nodeCount;

        return $this;
    }

    /**
     * Looks up the node for the given resource.
     * @param string $resource
     */
    public function lookup($resource)
    {
        $nodes = $this->lookupList($resource, 1);
        if (empty($nodes)) {
            throw new \Exception('No nodes exist');
        }

        return $nodes[0];
    }

    /**
     * Get a list of nodes for the resource, in order of precedence.
     * Up to $requestedCount nodes are returned, less if there are fewer in total.
     *
     * @param string $resource
     * @param int $requestedCount The length of the list to return
     * @return array List of nodes
     */
    public function lookupList($resource, $requestedCount)
    {
        if (!$requestedCount) {
            throw new \Exception('Invalid count requested');
        }

        // handle no nodes
        if (empty($this->positionToNode)) {
            return [];
        }

        // optimize single node
        if ($this->nodeCount == 1) {
            return array_unique(array_values($this->positionToNode));
        }

        // hash resource to a position
        $resourcePosition = $this->hasher->hash($resource);

        $results = [];
        $collect = false;

        $this->sortPositionNodes();

        // search values above the resourcePosition
        foreach ($this->positionToNode as $key => $value) {
            // start collecting nodes after passing resource position
            if (!$collect && $key > $resourcePosition) {
                $collect = true;
            }

            // only collect the first instance of any node
            if ($collect && !in_array($value, $results)) {
                $results [] = $value;
            }

            // return when enough results, or list exhausted
            if (count($results) == $requestedCount || count($results) == $this->nodeCount) {
                return $results;
            }
        }

        // loop to start - search values below the resourcePosition
        foreach ($this->positionToNode as $key => $value) {
            if (!in_array($value, $results)) {
                $results [] = $value;
            }

            // return when enough results, or list exhausted
            if (count($results) == $requestedCount || count($results) == $this->nodeCount) {
                return $results;
            }
        }

        // return results after iterating through both "parts"
        return $results;
    }

    /**
     * Sorts the mapping (positions to nodes) by position.
     */
    private function sortPositionNodes()
    {
        // sort by key (position) if not already
        if (!$this->positionToNodeSorted) {
            ksort($this->positionToNode, SORT_REGULAR);
            $this->positionToNodeSorted = true;
        }
    }
}