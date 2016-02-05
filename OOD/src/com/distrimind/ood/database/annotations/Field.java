/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */



package com.distrimind.ood.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation must be declared before a DatabaseRecord field. 
 * It is useless to use this annotation with the next annotations : {@link com.distrimind.ood.database.annotations.AutoPrimaryKey}, {@link com.distrimind.ood.database.annotations.RandomPrimaryKey}, {@link com.distrimind.ood.database.annotations.PrimaryKey}, {@link com.distrimind.ood.database.annotations.ForeignKey}. 
 * The accepted field types are all native java types, there correspondent class (i.e. 'Float' for 'float'), array of bytes, String, BigInteger, BigDecimal, and DatabaseRecord for foreign keys.
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Field {
    
    /**
     * 
     * @return The value limit in elements. This parameter concerns only the String type, the native byte array type, and the Object array type.
     */
    long limit() default 0;

}
