/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wildfly.transaction.client;


import com.arjuna.ats.internal.jta.resources.XAResourceErrorHandler;
import org.jboss.tm.XAResourceRecovery;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client._private.Log;

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * File that contains all outflowed resources info for a specific transaction.
 * This file must be created whenever a subordinate resource is outflowed to a remote location,
 * and deleted only when the transaction is committed or rolled back.
 *
 * Used for {@link #getXAResourceRecovery() recovery}.
 *
 * @author Flavia Rainone
 */
// TODO review not very good with class names... if the name doesn't sound right, any suggestions to naming this class are welcome
public final class XARecoveryRegistry {

    /**
     * Name of recovery dir. Location of this dir can be defined by {@link #setRelativeToPath(Path)}.
     */
    private static final String RECOVERY_DIR = "ejb-xa-recovery";

    /**
     * Empty utility array.
     */
    private static final XAResource[] EMPTY_IN_DOUBT_RESOURCES = new XAResource[0];

    /**
     * The xa recovery path, i.e., the path containing {@link #RECOVERY_DIR}. See {@link #setRelativeToPath}.
     */
    private static Path xaRecoveryPath = FileSystems.getDefault().getPath(RECOVERY_DIR);

    /**
     * A set containing the list of all registry files that are currently open. Used as a support to
     * identify in doubt registries. See {@link #recoverInDoubtRegistries()}.
     */
    private static Set<String> openFilePaths = Collections.synchronizedSet(new HashSet<>());

    /**
     * A list of in doubt resources, i.e., outflowed resources whose prepare/rollback/forget operation was not
     * completed normally, or resources that have been recovered from in doubt registries. See
     * {@link #inDoubtResource(SubordinateXAResource)} and {@link #loadInDoubtResources()}.
     */
    private static List<XAResource> inDoubtResources = Collections.synchronizedList(new ArrayList<>());

    /**
     * Path to the registry file.
     */
    @NotNull
    private final Path filePath;

    /**
     * The file channel, if non-null, it indicates that this registry represents a current, on-going transaction,
     * if null, then this registry represents an in-doubt registry recovered from file system.
     */
    private final FileChannel fileChannel;

    /**
     * Counts how many XA outflowed resources are stored in this registry, see {@link #addResource(URI)} and
     * {@link #removeResource(SubordinateXAResource)}.
     */
    private AtomicInteger resourceCount = new AtomicInteger();


    /**
     * Defines the path where recovery dir will be created. Cannot be defined after files have been created, or
     * recovery of those files could be lost.
     *
     * @param path the path recovery dir is relative to
     */
    public static void setRelativeToPath(Path path) {
        assert openFilePaths.isEmpty();
        xaRecoveryPath = path.resolve(RECOVERY_DIR);
    }

    /**
     * Creates a XA  recovery registry for a transaction. This method assumes that there is no file already
     * existing for this transaction, and, furthermore, it is not thread safe  (the creation of this object is
     * already thread protected at the caller, see {@link RemoteTransactionContext#outflowTransaction} and
     * {@link XAOutflowedResources#XAOutflowedResources(LocalTransaction)}.
     *
     * @param xid  the transaction xid
     * @throws SystemException if the there was a problem when creating the recovery file in file system
     */
    XARecoveryRegistry(Xid xid) throws SystemException {
        xaRecoveryPath.toFile().mkdir(); // create dir if non existent
        final String xidString = SimpleXid.of(xid).toHexString();
        this.filePath = xaRecoveryPath.resolve(xidString);
        openFilePaths.add(xidString);
        try {
            fileChannel = FileChannel.open(filePath, StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE_NEW);
            fileChannel.lock();
            Log.log.xaResourceRecoveryFileCreated(filePath);
        } catch (IOException e) {
            throw Log.log.createXAResourceRecoveryFileFailed(filePath, e);
        }
    }

    /**
     * Reload a registry that is in doubt, i.e., the registry is not associated yet with a current
     * transaction in this server, but with a transaction of a previous jvm instance that is now
     * being recovered.
     * This will happen only if the jvm crashes before a transaction with XA outflowed resources is
     * fully prepared. In this case, any lines in the registry can correspond to in doubt outflowed
     * resources. The goal is to reload those resources so they can be recovered by Arjuna.
     *
     * @param inDoubtFilePath the file path of the in doubt registry
     */
    private XARecoveryRegistry(String inDoubtFilePath) throws SystemException {
        this.filePath = xaRecoveryPath.resolve(inDoubtFilePath);
        this.fileChannel = null; // no need to open file channel here
        openFilePaths.add(inDoubtFilePath);
        loadInDoubtResources();
    }

    /**
     * Adds a subordinate XA resource to this registry.
     *
     * @param uri the outflowed resource URI location
     * @throws SystemException if there is a problem writing to the file
     */
    void addResource(URI uri) throws SystemException {
        if (fileChannel != null) {
            try {
                assert fileChannel.isOpen();
                fileChannel.write(ByteBuffer.wrap(uri.toString().getBytes()));
                fileChannel.force(true);
            } catch(IOException e) {
                throw Log.log.appendXAResourceRecoveryFileFailed(uri, filePath, e);
            }
        }
        this.resourceCount.getAndIncrement();
        Log.log.xaResourceAddedToRecoveryRegistry(uri, filePath);
    }

    /**
     * Removes a subordinate xa resource from this registry.
     *
     * The registry file is closed and deleted if there are no more resources left.
     *
     * @param resource the resource being removed
     * @throws XAException if there is a problem deleting the registry file
     */
    void removeResource(SubordinateXAResource resource) throws XAException {
        if (resourceCount.decrementAndGet() == 0) {
            // delete file
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
                Files.delete(filePath);
                openFilePaths.remove(filePath.toString());
            } catch (IOException e) {
                throw Log.log.deleteXAResourceRecoveryFileFailed(XAException.XAER_RMERR, filePath, resource, e);
            }
            Log.log.xaResourceRecoveryFileDeleted(filePath);
        }
        // remove resource from in doubt list, in case the resource was in doubt
        inDoubtResources.remove(resource);
    }

    /**
     * Flags the previously added resource as in doubt, meaning that it failed to complete prepare, rollback or forget.
     *
     * @param resource
     * @see #getXAResourceRecovery()
     */
    void inDoubtResource(SubordinateXAResource resource) {
        synchronized (inDoubtResources) {
            inDoubtResources.add(resource);
        }
    }

    /**
     * Loads in doubt resources from recovered registry file.
     *
     * @throws SystemException
     */
    private void loadInDoubtResources() throws SystemException {
        assert fileChannel == null;
        final List<String> uris;
        try {
            uris = Files.readAllLines(filePath);
        } catch (IOException e) {
            throw Log.log.readXAResourceRecoveryFileFailed(filePath, e);
        }
        final String nodeName = LocalTransactionContext.getCurrent().getProvider().getNodeName();
        for (String uriString: uris) {
            final URI uri;
            try {
                uri = new URI(uriString);
            } catch (URISyntaxException e) {
                throw Log.log.readURIFromXAResourceRecoveryFileFailed(uriString, filePath, e);
            }
            final XAResource xaresource = new SubordinateXAResource(uri, nodeName, this);
            inDoubtResources.add(xaresource);
            Log.log.xaResourceRecoveredFromRecoveryRegistry(uri, filePath);
        }
    }

    /**
     * Returns XAResourceRecovery for in doubt XAResources recovery.
     *
     * @return the XAResourceRecovery for in doubt subordinate XA resources recovery
     * @see org.jboss.tm.XAResourceRecoveryRegistry#addXAResourceRecovery(XAResourceRecovery)
     */
    public static XAResourceRecovery getXAResourceRecovery() {
        return () -> {
            try {
                recoverInDoubtRegistries();
            } catch (SystemException e) {
                // ignore wth a logged message
                Log.log.unexpectedExceptionOnXAResourceRecovery(e);
            }
            return inDoubtResources.isEmpty() ? EMPTY_IN_DOUBT_RESOURCES : inDoubtResources.toArray(
                    new XAResource[inDoubtResources.size()]);
        };
    }

    /**
     * Recovers closed registries from file system. All those registries are considered in doubt.
     *
     * @return the list of recovered registries
     * @throws SystemException if there was a problem when reading those files
     */
    private static List<XARecoveryRegistry> recoverInDoubtRegistries() throws SystemException {
        final File recoveryDir = xaRecoveryPath.toFile();
        final String[] xaRecoveryLogFileNames = recoveryDir.list();
        final List<XARecoveryRegistry> xaRecoveryRegistries = new ArrayList<>();
        for (int i = 0; i < xaRecoveryLogFileNames.length; i++) {
            // check if file is not open already
            if (!openFilePaths.contains(xaRecoveryLogFileNames[i]))
                xaRecoveryRegistries.add(new XARecoveryRegistry(xaRecoveryLogFileNames[i]));
        }
        return xaRecoveryRegistries;
    }

}
