package com.minikafka.broker;

import java.util.List;

/*
    Filing Cabinet (Partition) = one per topic Partition.
    Drawer (Segment) = A section inside the cabinet; when one drawer fills up (1MB), a new drawer is added.
    Letters (Messages) = the actual items stored inside a drawer (segment).
    Slot Number (Offset) = the position number of a specific letter within the cabinet (example: "Message #67")
    Reference Card (Index) = taped to each drawer, says "Message #47 is at position X inside this drawer."

    Cabinet → contains Drawers → Drawers contain Letters → each Letter has a Slot Number → a Reference Card tells you exactly where a Slot Number physically sits inside a Drawer.

    Log End Offset = the highest slot number given out so far (how many letters are filed). 
    Bouncer (ReadWriteLock) = controls door traffic: many people can read letters at once. But only one person can add a letter at a time. 
    Thread-safe Counter (AtomicLong) = the device that hands out slot numbers one at a time even if multiple people try to file a letter simultaneously.
*/

public class Partition {

    public Partition(int id, int leader, List<Integer> followers, String baseDirectory) {

    }

    /*
     * SegmentInfo = pointer to your segment (drawer) that is a .log file sitting on
     * disk.
     * 
     * logPath = address/key to physically open and access the drawer. Its a file
     * path (example: "/data/partition-0/000...000.log") where actual letters live.
     * 
     * indexPath = the address (file path) to a separate file on disk. That file
     * itself contains a sorted list of (offset → byte position) pairs — NOT the
     * letters themselves.
     * 
     * 
     * .log file = contains the ACTUAL letters (the data itself)
     * .index file = contains SHORTCUTS (byte positions) telling you WHERE in the
     * .log file each letter starts.
     * Without .index file you would have to iterate through each message to find
     * the message you are looking for in .log
     * 
     */
    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;

        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public String getLogPath() {
            return logPath;
        }

        public String getIndexPath() {
            return indexPath;
        }
    }
}